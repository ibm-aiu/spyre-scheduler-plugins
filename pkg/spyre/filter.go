/*
 * +-------------------------------------------------------------------+
 * | Copyright (c) 2025, 2026 IBM Corp.                                |
 * | SPDX-License-Identifier: Apache-2.0                               |
 * +-------------------------------------------------------------------+
 */

package spyre

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/kube-scheduler/framework"

	"k8s.io/klog/v2"

	spyrev1alpha1 "github.com/ibm-aiu/spyre-operator/api/v1alpha1"
	spyreconst "github.com/ibm-aiu/spyre-operator/const"
)

func (ap *SpyrePlugin) Filter(
	ctx context.Context, cycleState framework.CycleState,
	pod *corev1.Pod, nodeInfo framework.NodeInfo) *framework.Status {

	// Filter runs once per node for every Pod, so everything it logs is
	// multiplied by the number of nodes: keep it out of the default verbosity,
	// and log the Pod by reference rather than dumping the whole object.
	klog.V(4).InfoS("starting filter process", "pod", klog.KObj(pod), "node", nodeInfo.Node().Name)
	return ap.filter(ctx, pod, nodeInfo.Node().Name)
}

func (ap *SpyrePlugin) filter(ctx context.Context, pod *corev1.Pod, nodeName string) *framework.Status {

	klog.V(4).Info("checking number of requested spyre PFs/VFs")
	m, err := getNumRequestedDevices(pod)
	if err != nil {
		// The request itself is malformed; retrying cannot help.
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, err.Error())
	}

	if len(m) == 0 {
		klog.V(4).Info("skip filter operation because Pod does not request neither PF nor VF devices.")
		return framework.NewStatus(framework.Success)
	}

	klog.V(4).Info("check state in SpyreClusterPolicy")
	spyrepol, err := ap.policies.get(ctx)
	if err != nil {
		return framework.AsStatus(
			fmt.Errorf("failed to get SpyreClusterPolicy: %w", err))
	}

	nodeState, status := ap.nodeStateForFilter(ctx, pod, nodeName)
	if status != nil {
		return status
	}

	// card management Pod
	if isCardManagementPod(pod, spyrepol.Status.Namespace) {
		err = ap.selectOrRecheck(ctx, pod, nodeName, nodeState, func(s *spyrev1alpha1.SpyreNodeState) error {
			_, err := ap.SelectDevicesForCardManagement(pod, s, nodeName)
			return err
		})
		if err != nil {
			return selectionFailure(err, pod, nodeName,
				fmt.Sprintf("%s for card management Pod", errNotEnoughDevices))
		}
		return framework.NewStatus(framework.Success)
	}

	// user Pod
	if len(m) > 1 {
		return framework.NewStatus(framework.UnschedulableAndUnresolvable,
			"User Pod cannot request both PF and VF devices.")
	}

	var numRequested int64
	var resourceName string

	for k, v := range m {
		resourceName = k
		numRequested = int64(v)
	}

	klog.V(4).Infof("Pod requests %d %s.", numRequested, resourceName)

	isCardMgmtRunner := isCardManagementRunnerPod(pod, spyrepol.Status.Namespace)
	if !isCardMgmtRunner {
		if status := userPodAllowedOnNode(pod, nodeName, resourceName, spyrepol); status != nil {
			return status
		}
	}

	err = ap.selectOrRecheck(ctx, pod, nodeName, nodeState, func(s *spyrev1alpha1.SpyreNodeState) error {
		_, err := selectDevicesFromState(s, isCardMgmtRunner, resourceName, numRequested)
		return err
	})
	if err != nil {
		return selectionFailure(err, pod, nodeName,
			fmt.Sprintf("%s (%s): %d requested", errNotEnoughDevices, resourceName, numRequested))
	}
	return framework.NewStatus(framework.Success)
}

// selectOrRecheck runs selectFn against nodeState, which came from the cache.
//
// A shortage of devices is the one verdict worth double-checking. The cache can
// be behind the API, and a Pod that is rejected because of a release the cache
// has not seen yet is parked in the unschedulable queue: the SpyreNodeState
// update that would have woken it has already been delivered and judged, so only
// the periodic flush brings the Pod back, minutes later. So on a shortage the
// node state is read from the API once, and that verdict is the one that counts.
//
// The live copy is assumed into the cache, so that the next cycle starts from
// it. Filter still never writes to the API.
//
// When the live read fails, the shortage found in the cache stands: the Pod is
// rejected as it would have been, and the next SpyreNodeState event retries it.
func (ap *SpyrePlugin) selectOrRecheck(ctx context.Context, pod *corev1.Pod, nodeName string,
	nodeState *spyrev1alpha1.SpyreNodeState, selectFn func(*spyrev1alpha1.SpyreNodeState) error) error {

	err := selectFn(nodeState)
	if !errors.Is(err, errNotEnoughDevices) || !ap.nodeStates.servesFromCache() {
		return err
	}

	live, liveErr := ap.nodeStates.getLive(ctx, nodeName, nodeState.Name)
	if liveErr != nil {
		klog.V(4).InfoS("cannot recheck shortage against the API, keeping the cached verdict",
			"pod", klog.KObj(pod), "node", nodeName, "error", liveErr)
		return err
	}
	// Record the state as the API has it, before pruning edits it in memory.
	ap.nodeStates.assume(live)

	live.Status.NormalizeReservations()
	if pruneErr := ap.pruneStaleReservations(live, pod); pruneErr != nil {
		klog.V(4).InfoS("cannot prune the live node state, keeping the cached verdict",
			"pod", klog.KObj(pod), "node", nodeName, "error", pruneErr)
		return err
	}
	liveVerdict := selectFn(live)
	if liveVerdict == nil {
		klog.InfoS("stale SpyreNodeState cache detected: the API has room the cache did not",
			"pod", klog.KObj(pod), "node", nodeName,
			"cachedVersion", nodeState.ResourceVersion, "liveVersion", live.ResourceVersion)
	}
	return liveVerdict
}

// nodeStateForFilter returns the state of nodeName as Filter should judge it, or
// the status to return instead when it cannot be had.
//
// Filter only reads. Stale reservations are dropped from the copy it returns so
// that they do not count against the node; Reserve drops them for real when it
// writes the node state back, before it reserves anything on the node.
func (ap *SpyrePlugin) nodeStateForFilter(ctx context.Context, pod *corev1.Pod,
	nodeName string) (*spyrev1alpha1.SpyreNodeState, *framework.Status) {

	nodeState, err := ap.nodeStates.get(ctx, nodeName)
	if errors.Is(err, errNodeStateNotFound) {
		// Only this node is out of the question. An error status would abort
		// the scheduling of the Pod on every node.
		return nil, framework.NewStatus(framework.UnschedulableAndUnresolvable, err.Error())
	}
	if err != nil {
		return nil, framework.AsStatus(err)
	}
	nodeState.Status.NormalizeReservations()
	if err := ap.pruneStaleReservations(nodeState, pod); err != nil {
		return nil, framework.AsStatus(err)
	}
	return nodeState, nil
}

// userPodAllowedOnNode checks what the SpyreClusterPolicy says about a user Pod
// (not a card management Pod): that the policy is ready, and that the node may
// give out PFs. It returns the status to reject the node with, or nil.
func userPodAllowedOnNode(pod *corev1.Pod, nodeName, resourceName string,
	spyrepol *spyrev1alpha1.SpyreClusterPolicy) *framework.Status {

	// health-based filtering
	// An error status would send the Pod round the backoff queue for as
	// long as the policy stays unready; rejecting it instead parks it until
	// the policy changes.
	// Degraded still schedules: the per-node checks below keep the Pod off the
	// nodes that cannot serve it. Any other state (including ones added later)
	// is rejected.
	switch spyrepol.Status.State {
	case spyrev1alpha1.Ready, spyrev1alpha1.Degraded:
	default:
		return framework.NewStatus(framework.UnschedulableAndUnresolvable,
			fmt.Sprintf("SpyreClusterPolicy is not ready: %s", spyrepol.Status.State))
	}

	// cardmgmt-based filtering
	pfAvailable, msg := pfAvailableForUserPod(nodeName, spyrepol)
	if !pfAvailable && resourceName == spyreconst.PfResourceName {
		return framework.NewStatus(
			framework.UnschedulableAndUnresolvable,
			fmt.Sprintf("user Pod %s/%s cannot acquire PF device on node %s: %s",
				pod.Namespace, pod.Name, nodeName, msg))
	}

	klog.V(4).Infof("user Pod %s/%s is eligible to acquire device on node %s: %s",
		pod.Namespace, pod.Name, nodeName, msg)
	return nil
}

// selectionFailure rejects the node for a selection that failed with err.
//
// The scheduler writes the reason into the PodScheduled condition of the Pod, and
// patches the Pod whenever the reason differs from the one already there. When
// the node merely has too few free devices, err also says which devices are
// free, which changes from one attempt to the next; such a failure is reported
// as shortage, which only names the request, and the details are logged.
func selectionFailure(err error, pod *corev1.Pod, nodeName, shortage string) *framework.Status {
	if !errors.Is(err, errNotEnoughDevices) {
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, err.Error())
	}
	klog.V(4).InfoS("node has too few free devices", "pod", klog.KObj(pod), "node", nodeName, "detail", err.Error())
	return framework.NewStatus(framework.UnschedulableAndUnresolvable, shortage)
}

// pfAvailableForUserPod returns true if user Pod (not a cardmgmt Pod) can use PF devices.
// Second return value contains information about the true/false decision.
func pfAvailableForUserPod(nodeName string, spyrepol *spyrev1alpha1.SpyreClusterPolicy) (bool, string) {

	if !spyrepol.Spec.CardManagement.Enabled {
		return true, "cardmgmt is not enabled"
	}

	spyrefilter := "." // default value of cardmgmt
	if spyrepol.Spec.CardManagement.Config != nil &&
		spyrepol.Spec.CardManagement.Config.SpyreFilter != nil {
		spyrefilter = *spyrepol.Spec.CardManagement.Config.SpyreFilter
	}
	re, err := regexp.Compile(spyrefilter)
	if err != nil {
		return true, fmt.Sprintf(
			"bypassed due to cardmgmt configuration error (spyrefilter: %s)",
			spyrefilter)
	}

	if re.MatchString(nodeName) {
		return false, fmt.Sprintf(
			"cardmgmt manages node %s (spyrefilter: %s)",
			nodeName, spyrefilter)
	} else {
		return true, fmt.Sprintf(
			"cardmgmt does not manage node %s (spyrefilter: %s)",
			nodeName, spyrefilter)
	}

}
