#!/usr/bin/env bash
# +-------------------------------------------------------------------+
# | Copyright (c) 2026 IBM Corp.                                      |
# | SPDX-License-Identifier: Apache-2.0                               |
# +-------------------------------------------------------------------+
#
# Check the invariants that SpyreNodeState.status must satisfy.
#
# The one that matters most is that no device is held by two different Pods at
# once: that is the double allocation of ibm-aiu/spyre-operator#129, where a
# single-card job hung on open("/dev/vfio/<n>") because another Pod already had
# the card. The rest catch the ways the reservation bookkeeping can drift -
# entries that disagree with the deprecated deviceSets/podsUnderScheduling
# fields, reservations without a UID, and reservations whose owner Pod is long
# gone.
#
# Usage:
#   hack/spyre-nodestate-invariants.sh [-N node] [-g grace-seconds] [-q]
#   hack/spyre-nodestate-invariants.sh -f nodestate.json [-p pods.json]
#
#   -N node     check only this node (default: every SpyreNodeState)
#   -g seconds  reservation grace period, must match the scheduler's
#               SPYRE_RESERVATION_GRACE_PERIOD (default 30)
#   -f file     read SpyreNodeState(s) from a saved `kubectl get spyrens -o json`
#               instead of the live cluster
#   -p file     read the Pod list from a saved `kubectl get pods -A -o json`
#   -q          print nothing when every invariant holds
#
# Output is one line per violation, "SEVERITY<TAB>node<TAB>message".
# Exit status: 0 clean, 1 at least one FAIL, 2 only WARNs, 3 usage/setup error.

set -o nounset
set -o pipefail

NODE=""
GRACE=30
STATE_FILE=""
POD_FILE=""
QUIET=false

usage() {
	# -n plus a quit on the first non-comment line: BSD sed will not take a
	# negated address as the start of a range.
	sed -n -e '1,6d' -e '/^#/!q' -e 's/^# \{0,1\}//p' "${BASH_SOURCE[0]}"
	exit 3
}

while getopts ":N:g:f:p:qh" opt; do
	case "${opt}" in
	N) NODE="${OPTARG}" ;;
	g) GRACE="${OPTARG}" ;;
	f) STATE_FILE="${OPTARG}" ;;
	p) POD_FILE="${OPTARG}" ;;
	q) QUIET=true ;;
	*) usage ;;
	esac
done

KUBECTL="${KUBECTL:-$(command -v oc 2>/dev/null || command -v kubectl)}"
command -v jq >/dev/null || {
	echo "jq is required" >&2
	exit 3
}

TMPDIR_RUN=$(mktemp -d)
trap 'rm -rf "${TMPDIR_RUN}"' EXIT

# Collect the node states. A single -o json of a collection and of one object
# differ (List vs the object itself), so normalise to a list.
states="${TMPDIR_RUN}/states.json"
if [[ -n "${STATE_FILE}" ]]; then
	jq 'if .kind == "List" or has("items") then . else {items: [.]} end' "${STATE_FILE}" >"${states}" || exit 3
else
	if [[ -n "${NODE}" ]]; then
		"${KUBECTL}" get spyrens "${NODE}" -o json 2>/dev/null |
			jq '{items: [.]}' >"${states}" || {
			echo "cannot read SpyreNodeState ${NODE}" >&2
			exit 3
		}
	else
		"${KUBECTL}" get spyrens -o json >"${states}" || {
			echo "cannot list SpyreNodeStates" >&2
			exit 3
		}
	fi
fi

# The live Pod list is what tells an orphaned reservation from a live one. When
# checking a saved snapshot with no Pod list alongside it there is nothing to
# compare against, so those two checks are skipped rather than guessed at.
pods="${TMPDIR_RUN}/pods.json"
check_live=true
if [[ -n "${POD_FILE}" ]]; then
	cp "${POD_FILE}" "${pods}"
elif [[ -n "${STATE_FILE}" ]]; then
	check_live=false
	echo '{"items":[]}' >"${pods}"
else
	"${KUBECTL}" get pods --all-namespaces -o json >"${pods}" || {
		echo "cannot list Pods" >&2
		exit 3
	}
fi

read -r -d '' PROGRAM <<'JQ' || true
# podid identifies one generation of a Pod. The UID is the part that keeps a
# successor Pod from being mistaken for the predecessor whose name it reuses.
def podid: "\(.namespace // "-")/\(.name // "-")#\(.uid // "")";

# holders returns one record per (device, holder) pair on a node, drawn from both
# the allocations and the reservation entries.
def holders($s):
  ( ( ($s.status.allocation // [])
      | map( . as $a | ($a.devices // [])
             | map({ dev: ., kind: "allocation", pool: ($a.pool // "?"),
                     owner: (($a.pod // {}) | podid), reservedAt: null }) )
      | add ) // [] )
  + ( ( ($s.status.reservation // {}) | to_entries
        | map( .key as $pool | ( (.value.entries // [])
               | map( . as $e | ($e.devices // [])
                      | map({ dev: ., kind: "reservation", pool: $pool,
                              owner: ($e.pod | podid), reservedAt: $e.reservedAt }) )
               | add ) // [] )
        | add ) // [] );

def report($s; $live; $now; $grace; $checkLive):
  ($s.metadata.name) as $node
  | (holders($s)) as $h

  # The double allocation itself: one device recorded for more than one Pod.
  #
  # Two live Pods on one device is the bug, unambiguously. When only one of the
  # owners is still live the record is at worst lagging - a force-deleted Pod's
  # allocation that the pod watcher has not retired yet, or a reservation inside
  # its grace period - so it is reported as a WARN and left to the caller to
  # decide whether it persists. Without a Pod list to check liveness against,
  # every case has to be treated as the bug.
  | ( $h | group_by(.dev)
      | map(select((map(.owner) | unique | length) > 1))
      | map( . as $g
             | ($g | map(.owner) | unique) as $owners
             | ( if $checkLive then ($owners | map(. as $o | select($live | index($o)))) else $owners end ) as $liveOwners
             | ($g | map("\(.kind) in \(.pool)") | unique | join(", ")) as $where
             | if ($liveOwners | length) > 1
               then "FAIL\t\($node)\tdevice \($g[0].dev) is held by \($liveOwners | join(" AND ")) (\($where))"
               else "WARN\t\($node)\tdevice \($g[0].dev) is recorded for \($owners | join(" AND ")) but only \($liveOwners | length) of them is live (\($where)) - cleanup lag unless it persists"
               end ) )

  # A device allocated to a Pod that still has a reservation for it. The device
  # plugin retires the reservation in the same status update that adds the
  # allocation, so this should never be observed; if it is, one of the two write
  # paths is not going through the Reservation helpers.
  + ( $h | group_by(.dev)
      | map(select((map(.owner) | unique | length) == 1 and (map(.kind) | unique | length) > 1))
      | map("WARN\t\($node)\tdevice \(.[0].dev) is both allocated and reserved for \(.[0].owner)") )

  # No UID: either an older component wrote the record, or NormalizeFromLegacy
  # adopted a deviceSet it could not pair with a Pod. Such a record cannot tell
  # two generations of a name apart, which is the bug this all guards against.
  + ( $h | map(select(.owner | endswith("#"))) | unique_by([.kind, .pool, .owner])
      | map("WARN\t\($node)\t\(.kind) in \(.pool) for \(.owner) carries no Pod UID") )

  # Entries is the source of truth and SyncLegacy must have left the deprecated
  # fields derived from it. A mismatch means some writer edited one and not the
  # other, which is what makes a reader see a device as free.
  + ( ($s.status.reservation // {}) | to_entries
      | map( .key as $pool | .value as $r
             | ( (($r.entries // []) | map((.devices // []) | sort) | sort) ) as $fromEntries
             | ( (($r.deviceSets // []) | map(sort) | sort) ) as $legacy
             | if ($fromEntries | length) == 0 then
                 # No entries at all: the last writer only knew the deprecated
                 # fields. Legitimate mid-upgrade, and NormalizeFromLegacy will
                 # adopt them on the next read - but worth seeing, because until
                 # then those reservations have no owner identity.
                 ( if ($legacy | length) > 0 or (($r.podsUnderScheduling // []) | length) > 0
                   then ["WARN\t\($node)\tpool \($pool): reservation has deprecated fields but no entries - written by a component that predates ReservationEntry"]
                   else [] end )
               else
                 ( if $fromEntries != $legacy
                   then ["FAIL\t\($node)\tpool \($pool): deviceSets \($legacy | tojson) is not derived from entries \($fromEntries | tojson) - SyncLegacy was not called"]
                   else [] end )
                 + ( if (($r.entries // []) | length) != (($r.podsUnderScheduling // []) | length)
                     then ["FAIL\t\($node)\tpool \($pool): \((($r.podsUnderScheduling // []) | length)) podsUnderScheduling but \((($r.entries // []) | length)) entries"]
                     else [] end )
               end )
      | flatten )

  # A reservation whose owner is gone and whose grace period has elapsed should
  # have been released by the scheduler's next cleanup pass. One that lingers is
  # a leak: the devices stay unavailable to everyone.
  + ( if $checkLive | not then [] else
      ( $h | map(select(.kind == "reservation" and (.owner | endswith("#") | not)))
        | unique_by([.owner, .pool])
        | map( . as $e
               | if ($live | index($e.owner)) then empty
                 else ( if $e.reservedAt == null then 0 else ($e.reservedAt | fromdateiso8601) end ) as $t
                      | if $t == 0 or ($now - $t) > $grace
                        then "WARN\t\($node)\treservation in \($e.pool) for \($e.owner) outlived its Pod by more than \($grace)s"
                        else empty end
                 end ) )
      + ( $h | map(select(.kind == "allocation" and (.owner | endswith("#") | not)))
          | unique_by(.owner)
          | map(. as $a | select(($live | index($a.owner)) | not))
          | map("WARN\t\($node)\tallocation in \(.pool) for \(.owner) has no live Pod") )
      end );

( $pods[0].items // [] | map((.metadata | {name, namespace, uid}) | podid) ) as $live
| ( $states[0].items // [] )
| map(report(.; $live; $now; $grace; $checkLive))
| flatten
| .[]
JQ

now=$(date -u +%s)
violations=$(jq -r -n \
	--slurpfile states "${states}" \
	--slurpfile pods "${pods}" \
	--argjson now "${now}" \
	--argjson grace "${GRACE}" \
	--argjson checkLive "${check_live}" \
	"${PROGRAM}")
rc=$?
if ((rc != 0)); then
	echo "invariant check could not run (jq exit ${rc})" >&2
	exit 3
fi

if [[ -z "${violations}" ]]; then
	"${QUIET}" || echo "OK	all SpyreNodeState invariants hold"
	exit 0
fi

echo "${violations}"
if grep -q '^FAIL' <<<"${violations}"; then
	exit 1
fi
exit 2
