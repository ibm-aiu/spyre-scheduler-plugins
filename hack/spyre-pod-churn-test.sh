#!/usr/bin/env bash
# +-------------------------------------------------------------------+
# | Copyright (c) 2026 IBM Corp.                                      |
# | SPDX-License-Identifier: Apache-2.0                               |
# +-------------------------------------------------------------------+
#
# Recreate a Pod under the same name as fast as the API server allows, and check
# after every step that no Spyre device ends up held by two Pods at once.
#
# This is the shape of the workload that exposed ibm-aiu/spyre-operator#129: a
# GitHub Actions runner set replaces its runner Pods every few seconds, always
# under the same name, and each runner asks for one card. Two things about it
# defeat name-only bookkeeping. The successor shares its predecessor's
# namespace/name, so a reservation recorded for the old Pod looks like it belongs
# to the new one; and while several single-card reservations are outstanding, the
# reserved device sets are all of size one and cannot be told apart by size. Both
# are why a reservation now carries the owner's UID and its own device list.
#
# The test therefore needs three things at the same time:
#
#   * churn workers (-c, at least 2) that each recreate one fixed name in a tight
#     loop, so more than one single-card reservation is outstanding at a time;
#   * holder Pods (-H) that sit still holding one card each and must never lose
#     it - if the scheduler releases the wrong reservation, a churn Pod lands on
#     a holder's card and the holder's container blocks on open("/dev/vfio/<n>");
#   * the invariant check, run continuously against SpyreNodeState.
#
# Deletion is `--force --grace-period=0` by default, which is the harsh case on
# purpose: the API object disappears at once while the container may still hold
# the device open. That is exactly what the reservation grace period covers, so
# -g must match the scheduler's SPYRE_RESERVATION_GRACE_PERIOD.
#
# Usage: hack/spyre-pod-churn-test.sh [options]
#
#   -n namespace   test namespace, created and deleted (default spyre-churn-test)
#   -N node        node to run on (default: the first node advertising -r)
#   -r resource    resource to request (default ibm.com/spyre_pf)
#   -i iterations  recreations per churn worker (default 20)
#   -c workers     concurrent churn workers, each with its own fixed Pod name
#                  (default 2 - one worker cannot reproduce the ambiguity)
#   -H holders     long-lived single-card Pods that must keep their card
#                  (default 1)
#   -g seconds     reservation grace period of the scheduler under test
#                  (default 30, i.e. the built-in default)
#   -m mode        force (default) or graceful deletion
#   -I image       container image (default ubi9/ubi-minimal)
#   -s scheduler   .spec.schedulerName (default spyre-scheduler)
#   -O             skip the in-container open() check on /dev/vfio/*
#   -k             keep the namespace and the artifacts on exit
#   -a dir         artifact directory (default ./spyre-churn-<timestamp>)
#
# Every Pod requests exactly one device: a single-card request is what makes two
# outstanding reservations indistinguishable by size, and it is what the reported
# failures were.
#
# Exit status: 0 no violation, 1 at least one FAIL, 3 setup error.

# errexit is deliberately off: a create that loses the race against a Terminating
# Pod, and a delete of a Pod that has already gone, are both expected here and
# are handled where they happen.
set -o nounset
set -o pipefail

NAMESPACE="spyre-churn-test"
NODE=""
RESOURCE="ibm.com/spyre_pf"
ITERATIONS=20
WORKERS=2
HOLDERS=1
GRACE=30
DELETE_MODE="force"
IMAGE="registry.access.redhat.com/ubi9/ubi-minimal:latest"
SCHEDULER="spyre-scheduler"
OPEN_CHECK=true
KEEP=false
ARTIFACTS=""

# How long a churn Pod is given to reach Running before it counts as a failure.
# A Pod that never gets there is itself a finding: it usually means reservations
# leaked and the node no longer looks like it has a free card.
POD_READY_TIMEOUT="${POD_READY_TIMEOUT:-120}"
# How long to keep retrying create while the predecessor is still Terminating.
RECREATE_TIMEOUT="${RECREATE_TIMEOUT:-120}"
# Seconds between invariant checks while the churn runs. Each check lists the
# node state and the cluster's Pods, so do not make this much smaller.
MONITOR_INTERVAL="${MONITOR_INTERVAL:-2}"

usage() {
	# -n plus a quit on the first non-comment line: BSD sed will not take a
	# negated address as the start of a range.
	sed -n -e '1,6d' -e '/^#/!q' -e 's/^# \{0,1\}//p' "${BASH_SOURCE[0]}"
	exit 3
}

while getopts ":n:N:r:i:c:H:g:m:I:s:a:Okh" opt; do
	case "${opt}" in
	n) NAMESPACE="${OPTARG}" ;;
	N) NODE="${OPTARG}" ;;
	r) RESOURCE="${OPTARG}" ;;
	i) ITERATIONS="${OPTARG}" ;;
	c) WORKERS="${OPTARG}" ;;
	H) HOLDERS="${OPTARG}" ;;
	g) GRACE="${OPTARG}" ;;
	m) DELETE_MODE="${OPTARG}" ;;
	I) IMAGE="${OPTARG}" ;;
	s) SCHEDULER="${OPTARG}" ;;
	a) ARTIFACTS="${OPTARG}" ;;
	O) OPEN_CHECK=false ;;
	k) KEEP=true ;;
	*) usage ;;
	esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHECKER="${REPO_ROOT}/hack/spyre-nodestate-invariants.sh"
KUBECTL="${KUBECTL:-$(command -v oc 2>/dev/null || command -v kubectl)}"
# The checker resolves its own client, so hand it this one: the two must agree
# about which cluster is under test.
export KUBECTL
: "${ARTIFACTS:=./spyre-churn-$(date -u +%Y%m%dT%H%M%SZ)}"

log() { printf '%s %s\n' "$(date -u +%T)" "$*"; }
fail() { printf '%s FAIL %s\n' "$(date -u +%T)" "$*" | tee -a "${ARTIFACTS}/failures.log"; }

# ---------------------------------------------------------------- preflight ---

[[ -x "${CHECKER}" ]] || chmod +x "${CHECKER}" 2>/dev/null
[[ -r "${CHECKER}" ]] || {
	echo "cannot find ${CHECKER}" >&2
	exit 3
}
for c in jq "${KUBECTL}"; do
	command -v "${c}" >/dev/null || {
		echo "${c} is required" >&2
		exit 3
	}
done
((WORKERS >= 1)) || usage
[[ "${DELETE_MODE}" == "force" || "${DELETE_MODE}" == "graceful" ]] || usage
((WORKERS >= 2)) || log "NOTE: -c 1 cannot produce two outstanding same-size reservations; the ambiguity this test targets needs at least 2"

"${KUBECTL}" get crd spyrenodestates.spyre.ibm.com >/dev/null 2>&1 || {
	echo "SpyreNodeState CRD not found - is the operator installed?" >&2
	exit 3
}

# Pick a node that advertises the resource, and check it has enough cards for the
# holders plus one per churn worker. Without a spare card the churn Pods simply
# queue and nothing interesting is exercised.
node_json="$("${KUBECTL}" get nodes -o json)"
if [[ -z "${NODE}" ]]; then
	NODE=$(jq -r --arg r "${RESOURCE}" \
		'[.items[] | select((.status.allocatable[$r] // "0") | tonumber > 0)][0].metadata.name // ""' \
		<<<"${node_json}")
fi
[[ -n "${NODE}" ]] || {
	echo "no node advertises ${RESOURCE}" >&2
	exit 3
}
CAPACITY=$(jq -r --arg n "${NODE}" --arg r "${RESOURCE}" \
	'.items[] | select(.metadata.name == $n) | .status.allocatable[$r] // "0"' <<<"${node_json}")
log "node ${NODE} advertises ${CAPACITY} x ${RESOURCE}"
NEEDED=$((HOLDERS + WORKERS))
if ((CAPACITY < NEEDED)); then
	echo "node ${NODE} has ${CAPACITY} devices but ${NEEDED} are needed (${HOLDERS} holders + ${WORKERS} churn workers)" >&2
	echo "reduce -H/-c, or pick another node with -N" >&2
	exit 3
fi

mkdir -p "${ARTIFACTS}"
log "artifacts in ${ARTIFACTS}"

cleanup() {
	local rc=$?
	set +o nounset
	# Kill any worker still looping before tearing the namespace down, so the
	# recreate retry loop does not fight the namespace deletion.
	for pid in ${WORKER_PIDS:-}; do kill "${pid}" 2>/dev/null; done
	wait 2>/dev/null
	snapshot final
	if "${KEEP}"; then
		log "keeping namespace ${NAMESPACE} (-k)"
	else
		log "deleting namespace ${NAMESPACE}"
		"${KUBECTL}" delete namespace "${NAMESPACE}" --wait=false >/dev/null 2>&1
	fi
	exit "${rc}"
}

# ------------------------------------------------------------------ manifest ---

container_script() {
	if "${OPEN_CHECK}"; then
		cat <<'SCRIPT'
echo "[pod] start name=${POD_NAME} uid=${POD_UID} node=${NODE_NAME}"
cat /etc/aiu/resource_pool 2>/dev/null || true
ls -l /dev/vfio 2>/dev/null || echo "[pod] no /dev/vfio"
# Opening the assigned device is what a real workload does first, and what hangs
# when the card was handed to somebody else as well. A timeout turns that hang
# into an observable result instead of a Pod that merely looks Running.
for d in /dev/vfio/*; do
  [ -e "$d" ] || continue
  [ "$d" = "/dev/vfio/vfio" ] && continue
  if ! command -v timeout >/dev/null 2>&1; then echo "[pod] OPEN_SKIP $d (no timeout binary)"; continue; fi
  if timeout 10 sh -c "exec 3< $d" 2>/tmp/openerr; then
    echo "[pod] OPEN_OK $d"
  else
    rc=$?
    if [ "$rc" -eq 124 ]; then
      echo "[pod] OPEN_TIMEOUT $d - device is busy elsewhere"
    else
      echo "[pod] OPEN_ERR $d rc=$rc $(cat /tmp/openerr 2>/dev/null)"
    fi
  fi
done
echo "[pod] READY"
exec sleep 86400
SCRIPT
	else
		cat <<'SCRIPT'
echo "[pod] start name=${POD_NAME} uid=${POD_UID} node=${NODE_NAME}"
ls -l /dev/vfio 2>/dev/null || echo "[pod] no /dev/vfio"
echo "[pod] READY"
exec sleep 86400
SCRIPT
	fi
}

pod_manifest() {
	local name="$1" role="$2"
	cat <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${name}
  namespace: ${NAMESPACE}
  labels:
    spyre-churn-test: "true"
    spyre-churn-role: "${role}"
spec:
  schedulerName: ${SCHEDULER}
  restartPolicy: Never
  terminationGracePeriodSeconds: 0
  nodeSelector:
    kubernetes.io/hostname: ${NODE}
  containers:
    - name: app
      image: ${IMAGE}
      env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_UID
          valueFrom:
            fieldRef:
              fieldPath: metadata.uid
        - name: NODE_NAME
          valueFrom:
            fieldRef:
              fieldPath: spec.nodeName
      command:
        - /bin/sh
        - -c
        - |
$(container_script | sed 's/^/          /')
      resources:
        limits:
          ${RESOURCE}: "1"
EOF
}

# --------------------------------------------------------------- primitives ---

# create_pod retries while the predecessor of the same name is still Terminating.
# The retry is deliberately tight: the point is to take the new Pod the instant
# the old object is gone, which is the smallest gap the API server permits and
# the window in which name-only bookkeeping goes wrong.
create_pod() {
	local name="$1" role="$2" out deadline=$((SECONDS + RECREATE_TIMEOUT))
	local manifest
	manifest="$(pod_manifest "${name}" "${role}")"
	while :; do
		out=$("${KUBECTL}" create -f - <<<"${manifest}" 2>&1)
		if (($? == 0)); then return 0; fi
		if [[ "${out}" == *AlreadyExists* || "${out}" == *"being deleted"* ]]; then
			((SECONDS > deadline)) && {
				echo "${out}" >&2
				return 1
			}
			continue
		fi
		echo "${out}" >&2
		return 1
	done
}

delete_pod() {
	local name="$1"
	if [[ "${DELETE_MODE}" == "force" ]]; then
		"${KUBECTL}" -n "${NAMESPACE}" delete pod "${name}" \
			--force --grace-period=0 --wait=false >/dev/null 2>&1
	else
		"${KUBECTL}" -n "${NAMESPACE}" delete pod "${name}" --wait=false >/dev/null 2>&1
	fi
}

pod_field() {
	"${KUBECTL}" -n "${NAMESPACE}" get pod "$1" -o "jsonpath={$2}" 2>/dev/null
}

# wait_running returns 0 once the Pod is Running, 1 on Failed and 2 on timeout.
wait_running() {
	local name="$1" deadline=$((SECONDS + POD_READY_TIMEOUT)) phase
	while ((SECONDS <= deadline)); do
		phase="$(pod_field "${name}" .status.phase)"
		case "${phase}" in
		Running | Succeeded) return 0 ;;
		Failed) return 1 ;;
		esac
		sleep 0.2
	done
	return 2
}

snapshot() {
	# Declared separately: under `set -u`, bash 3.2 cannot expand a name that the
	# same `local` statement is still assigning.
	local tag="$1"
	local dir="${ARTIFACTS}/${tag}"
	mkdir -p "${dir}"
	"${KUBECTL}" get spyrens "${NODE}" -o json >"${dir}/spyrenodestate.json" 2>/dev/null
	"${KUBECTL}" -n "${NAMESPACE}" get pods -o json >"${dir}/pods.json" 2>/dev/null
	"${KUBECTL}" -n "${NAMESPACE}" get events --sort-by=.lastTimestamp >"${dir}/events.txt" 2>/dev/null
	"${KUBECTL}" -n "${NAMESPACE}" get pods -o wide >"${dir}/pods.txt" 2>/dev/null
}

# devices_of prints the devices SpyreNodeState records for one Pod generation,
# from the allocation first and from the reservation entries otherwise.
devices_of() {
	local name="$1" uid="$2"
	"${KUBECTL}" get spyrens "${NODE}" -o json 2>/dev/null | jq -r \
		--arg ns "${NAMESPACE}" --arg n "${name}" --arg u "${uid}" '
      def mine: .pod != null and .pod.namespace == $ns and .pod.name == $n
                and (.pod.uid == "" or .pod.uid == null or .pod.uid == $u);
      ( [ (.status.allocation // [])[] | select(mine) | .devices[]? ]
        + [ (.status.reservation // {})[] | (.entries // [])[] | select(mine) | .devices[]? ] )
      | unique | join(",")'
}

# check_invariants runs the checker once and, if it reports a FAIL, runs it again
# a moment later. Only a violation that survives the second look is counted: a
# single sample can catch a status update mid-flight, and reporting that as the
# bug would bury the real thing in noise.
check_invariants() {
	local tag="$1" out rc again
	out=$("${CHECKER}" -N "${NODE}" -g "${GRACE}" -q 2>&1)
	rc=$?
	case "${rc}" in
	1)
		sleep 2
		again=$("${CHECKER}" -N "${NODE}" -g "${GRACE}" -q 2>&1)
		if grep -q '^FAIL' <<<"${again}"; then
			fail "invariant violated (${tag}), confirmed on re-check:"
			printf '%s\n' "${again}" | tee -a "${ARTIFACTS}/failures.log"
			snapshot "violation-${tag}"
			return 1
		fi
		{
			echo "--- transient at ${tag} ---"
			printf '%s\n' "${out}"
		} >>"${ARTIFACTS}/transient.log"
		;;
	2) printf '%s\n' "${out}" >>"${ARTIFACTS}/warnings.log" ;;
	0) ;;
	*) log "checker error: ${out}" ;;
	esac
	return 0
}

# ------------------------------------------------------------------- workers ---

# churn_worker recreates one fixed Pod name ITERATIONS times. Findings go to its
# own log with a FAIL prefix; the main loop collects them at the end.
churn_worker() {
	local slot="$1"
	local name="churn-${slot}"
	local logf="${ARTIFACTS}/worker-${slot}.log"
	local i uid devs rc
	for ((i = 1; i <= ITERATIONS; i++)); do
		if ! create_pod "${name}" churn 2>>"${logf}"; then
			echo "FAIL iter=${i} could not create ${name}" >>"${logf}"
			return
		fi
		uid="$(pod_field "${name}" .metadata.uid)"
		wait_running "${name}"
		rc=$?
		devs="$(devices_of "${name}" "${uid}")"
		case "${rc}" in
		0) echo "iter=${i} uid=${uid} running devices=${devs:-none}" >>"${logf}" ;;
		1)
			echo "FAIL iter=${i} uid=${uid} pod Failed devices=${devs:-none}" >>"${logf}"
			"${KUBECTL}" -n "${NAMESPACE}" logs "${name}" >>"${logf}" 2>&1
			;;
		2)
			# Still Pending after the timeout. Either the scheduler cannot find a
			# free card - reservations leaked - or the kubelet could not allocate.
			echo "FAIL iter=${i} uid=${uid} pod not Running within ${POD_READY_TIMEOUT}s phase=$(pod_field "${name}" .status.phase) devices=${devs:-none}" >>"${logf}"
			"${KUBECTL}" -n "${NAMESPACE}" describe pod "${name}" >>"${logf}" 2>&1
			;;
		esac
		# A container that got a card somebody else is using blocks in open()
		# rather than failing, so the Pod looks healthy. The log is the evidence.
		if "${OPEN_CHECK}" && ((rc == 0)); then
			if "${KUBECTL}" -n "${NAMESPACE}" logs "${name}" 2>/dev/null | grep -q OPEN_TIMEOUT; then
				echo "FAIL iter=${i} uid=${uid} open() on its device blocked - devices=${devs}" >>"${logf}"
				"${KUBECTL}" -n "${NAMESPACE}" logs "${name}" >>"${logf}" 2>&1
			fi
		fi
		delete_pod "${name}"
	done
	echo "done ${ITERATIONS} iterations" >>"${logf}"
}

# --------------------------------------------------------------------- main ---

trap cleanup EXIT INT TERM

"${KUBECTL}" create namespace "${NAMESPACE}" >/dev/null 2>&1 ||
	log "namespace ${NAMESPACE} already exists, reusing it"

# A node that already breaks the invariants makes everything that follows
# unreadable, so stop rather than add churn on top of it.
log "baseline invariant check"
if ! check_invariants baseline; then
	fail "node ${NODE} already violates the invariants before any churn - investigate ${ARTIFACTS}/violation-baseline first"
	KEEP=true
	exit 1
fi

# The holders are the victims to watch: they never move, so any change in the
# devices recorded for them means somebody else was given their card.
# Indexed arrays rather than an associative one, so this still runs under the
# bash 3.2 that ships with macOS.
HOLDER_NAMES=()
HOLDER_DEVS=()
for ((h = 0; h < HOLDERS; h++)); do
	hname="holder-${h}"
	create_pod "${hname}" holder || exit 3
	if ! wait_running "${hname}"; then
		fail "holder ${hname} did not reach Running"
		"${KUBECTL}" -n "${NAMESPACE}" describe pod "${hname}" >>"${ARTIFACTS}/failures.log" 2>&1
		exit 1
	fi
	huid="$(pod_field "${hname}" .metadata.uid)"
	HOLDER_NAMES[h]="${hname}"
	HOLDER_DEVS[h]="$(devices_of "${hname}" "${huid}")"
	log "holder ${hname} holds [${HOLDER_DEVS[h]}]"
done

snapshot before

WORKER_PIDS=""
for ((w = 0; w < WORKERS; w++)); do
	churn_worker "${w}" &
	WORKER_PIDS+=" $!"
done
log "started ${WORKERS} churn worker(s) x ${ITERATIONS} iterations, delete mode=${DELETE_MODE}"

# While the workers churn, check the invariants and the holders continuously.
# Checking from here rather than inside a worker keeps one consistent view of the
# node state instead of several racing ones.
VIOLATIONS=0
while :; do
	running=false
	for pid in ${WORKER_PIDS}; do
		kill -0 "${pid}" 2>/dev/null && running=true
	done
	check_invariants churn || VIOLATIONS=$((VIOLATIONS + 1))
	for ((h = 0; h < ${#HOLDER_NAMES[@]}; h++)); do
		hname="${HOLDER_NAMES[h]}"
		phase="$(pod_field "${hname}" .status.phase)"
		if [[ "${phase}" != "Running" ]]; then
			fail "holder ${hname} left Running (phase=${phase}) - it should never be disturbed"
			VIOLATIONS=$((VIOLATIONS + 1))
		fi
		now_devs="$(devices_of "${hname}" "$(pod_field "${hname}" .metadata.uid)")"
		if [[ -n "${now_devs}" && "${now_devs}" != "${HOLDER_DEVS[h]}" ]]; then
			fail "holder ${hname} devices changed from [${HOLDER_DEVS[h]}] to [${now_devs}]"
			VIOLATIONS=$((VIOLATIONS + 1))
		fi
	done
	"${running}" || break
	sleep "${MONITOR_INTERVAL}"
done
wait

# After the churn stops, the reservations for every deleted Pod must drain within
# the grace period. Anything still there afterwards is a leak that will
# eventually make the node look full.
log "churn finished; waiting $((GRACE + 15))s for reservations to drain"
sleep "$((GRACE + 15))"
check_invariants drain || VIOLATIONS=$((VIOLATIONS + 1))
leftover=$("${KUBECTL}" get spyrens "${NODE}" -o json |
	jq -r --arg ns "${NAMESPACE}" '
    [ (.status.reservation // {}) | to_entries[]
      | .key as $p | (.value.entries // [])[]
      | select(.pod.namespace == $ns and (.pod.name | startswith("churn-")))
      | "\($p): \(.pod.name)#\(.pod.uid) \(.devices | join(","))" ] | .[]')
if [[ -n "${leftover}" ]]; then
	fail "reservations for deleted churn Pods survived the grace period:"
	printf '%s\n' "${leftover}" | tee -a "${ARTIFACTS}/failures.log"
	VIOLATIONS=$((VIOLATIONS + 1))
fi

# ------------------------------------------------------------------ summary ---

echo
log "==== summary ===="
for ((w = 0; w < WORKERS; w++)); do
	logf="${ARTIFACTS}/worker-${w}.log"
	[[ -r "${logf}" ]] || continue
	iters=$(grep -c '^iter=' "${logf}")
	wfails=$(grep -c '^FAIL' "${logf}")
	log "worker ${w}: ${iters} iterations, ${wfails} failure(s)"
	((wfails > 0)) && grep '^FAIL' "${logf}" | sed 's/^/    /'
	VIOLATIONS=$((VIOLATIONS + wfails))
done
for ((h = 0; h < ${#HOLDER_NAMES[@]}; h++)); do
	log "holder ${HOLDER_NAMES[h]}: devices [${HOLDER_DEVS[h]}], final phase $(pod_field "${HOLDER_NAMES[h]}" .status.phase)"
done
if [[ -s "${ARTIFACTS}/warnings.log" ]]; then
	log "warnings (see ${ARTIFACTS}/warnings.log):"
	sort -u "${ARTIFACTS}/warnings.log" | sed 's/^/    /'
fi

if ((VIOLATIONS > 0)); then
	log "RESULT: FAIL - ${VIOLATIONS} violation(s), details in ${ARTIFACTS}"
	KEEP=true
	exit 1
fi
log "RESULT: PASS - no device was ever held by two Pods"
exit 0
