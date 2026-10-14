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

	klog.InfoS("starting filter process", "pod", pod, "node", nodeInfo.Node().Name)
	return ap.filter(ctx, pod, nodeInfo.Node().Name)
}

func (ap *SpyrePlugin) filter(ctx context.Context, pod *corev1.Pod, nodeName string) *framework.Status {

	klog.Info("checking number of requested spyre PFs/VFs")
	m, err := getNumRequestedDevices(pod)
	if err != nil {
		// The request itself is malformed; retrying cannot help.
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, err.Error())
	}

	if len(m) == 0 {
		klog.Info("skip filter operation because Pod does not request neither PF nor VF devices.")
		return framework.NewStatus(framework.Success)
	}

	klog.Info("check state in SpyreClusterPolicy")
	spyrepol, err := ap.policies.get(ctx)
	if err != nil {
		return framework.AsStatus(
			fmt.Errorf("failed to get SpyreClusterPolicy: %w", err))
	}

	// Filter only reads. Stale reservations are dropped from this copy so that
	// they do not count against the node; Reserve drops them for real when it
	// writes the node state back, before it reserves anything on the node.
	nodeState, err := ap.nodeStates.get(ctx, nodeName)
	if errors.Is(err, errNodeStateNotFound) {
		// Only this node is out of the question. An error status would abort
		// the scheduling of the Pod on every node.
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, err.Error())
	}
	if err != nil {
		return framework.AsStatus(err)
	}
	nodeState.Status.NormalizeReservations()
	if err := ap.pruneStaleReservations(nodeState, pod); err != nil {
		return framework.AsStatus(err)
	}

	// card management Pod
	if isCardManagementPod(pod, spyrepol.Status.Namespace) {
		if _, err = ap.SelectDevicesForCardManagement(pod, nodeState, nodeName); err != nil {
			return framework.NewStatus(framework.UnschedulableAndUnresolvable, err.Error())
		} else {
			return framework.NewStatus(framework.Success)
		}
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

	klog.Infof("Pod requests %d %s.", numRequested, resourceName)

	isCardMgmtRunner := isCardManagementRunnerPod(pod, spyrepol.Status.Namespace)

	if !isCardMgmtRunner { // user Pod

		// health-based filtering
		// An error status would send the Pod round the backoff queue for as
		// long as the policy stays unready; rejecting it instead parks it until
		// the policy changes.
		if spyrepol.Status.State != spyrev1alpha1.Ready {
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

		klog.Infof("user Pod %s/%s is eligible to acquire device on node %s: %s",
			pod.Namespace, pod.Name, nodeName, msg)
	}

	if _, err = selectDevicesFromState(nodeState, isCardMgmtRunner, resourceName, numRequested); err != nil {
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, err.Error())
	}
	return framework.NewStatus(framework.Success)
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
