/*
 * +-------------------------------------------------------------------+
 * | Copyright (c) 2025, 2026 IBM Corp.                                |
 * | SPDX-License-Identifier: Apache-2.0                               |
 * +-------------------------------------------------------------------+
 */

package spyre

import (
	"context"
	"fmt"

	spyrev1alpha1 "github.com/ibm-aiu/spyre-operator/api/v1alpha1"
	spyreclient "github.com/ibm-aiu/spyre-operator/pkg/client"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/kube-scheduler/framework"
)

// this file implements ReservePlugin interface of the scheduling framework.

// Reserve converts per-device-type resource requests to per-device request in the Pod, and
// then add the devices to reservedSpyreInterfaces in SpyreNodeState.
func (ap *SpyrePlugin) Reserve(
	ctx context.Context, state framework.CycleState, p *corev1.Pod, nodeName string) *framework.Status {
	klog.Info("start the process in Reserve extension point")

	klog.Info("trying to reserve requested devices")
	err := ap.reserveDevices(ctx, p, nodeName)
	if err != nil {
		return framework.NewStatus(framework.Error, err.Error())
	}

	return framework.NewStatus(framework.Success)
}

func (ap *SpyrePlugin) reserveDevices(ctx context.Context, p *corev1.Pod, nodeName string) error {

	klog.Info("getting number of requested devices")
	m, err := getNumRequestedDevices(p)
	if err != nil {
		return err
	}

	if len(m) == 0 {
		klog.Info("skip reserve operation because Pod does not request neither PF nor VF devices.")
		return nil
	}

	var nReq int64
	var rName string

	for k, v := range m {
		rName = k
		nReq = int64(v)
	}
	klog.Infof("Spyre requests: %d (%v)", nReq, rName)

	klog.Info("getting SpyreClusterPolicy")
	spyrepol, err := ap.spyreClient.GetSpyreClusterPolicy(ctx, "spyreclusterpolicy")
	if err != nil {
		return fmt.Errorf("failed to get AiuClusterPolicy: %w", err)
	}

	if !isCardManagementPod(p, spyrepol.Status.Namespace) && len(m) > 1 {
		return fmt.Errorf("User Pod cannot request both PF and VF devices.")
	}

	// Pruning stale reservations, choosing devices and recording the reservation
	// all happen inside a single read-modify-write cycle, so the devices are
	// always chosen from the very state that is then written back. On a conflict
	// the whole closure runs again against freshly read state.
	//
	// The previous implementation chose devices once and then retried the write
	// up to ten times with the resourceVersion it had read before choosing: once
	// another scheduling cycle had updated the node state every one of those
	// retries was doomed, which is where "retry number exceeded" came from.
	_, err = ap.spyreClient.MutateNodeStateStatus(ctx, nodeName,
		func(nodeState *spyrev1alpha1.SpyreNodeState) error {
			klog.Infof("reservation (prior): %v", nodeState.Status.Reservations)

			if err := ap.pruneStaleReservations(ctx, nodeState, p); err != nil {
				return err
			}

			rDevs, err := ap.chooseDevicesToReserve(ctx, p, nodeState, nodeName, spyrepol, rName, nReq)
			if err != nil {
				return err
			}

			reservedAt := metav1.Now()
			for name, devs := range rDevs {
				nodeState.Status.ReserveDevices(name, podReference(p), devs, reservedAt)
			}
			klog.Infof("reservation: %v", nodeState.Status.Reservations)
			return nil
		})
	if err != nil {
		klog.ErrorS(err, "failed to reserve devices", "pod", klog.KObj(p), "node", nodeName)
		return err
	}

	klog.Info("reservation successfully finished")
	return nil
}

// chooseDevicesToReserve picks the devices to reserve for p from nodeState, and
// returns them keyed by resource name. It only reads nodeState: the caller is
// responsible for recording the reservation and writing it back.
func (ap *SpyrePlugin) chooseDevicesToReserve(ctx context.Context, p *corev1.Pod,
	nodeState *spyrev1alpha1.SpyreNodeState, nodeName string,
	spyrepol *spyrev1alpha1.SpyreClusterPolicy, rName string, nReq int64) (map[string][]string, error) {

	if isCardManagementPod(p, spyrepol.Status.Namespace) {
		klog.Info("reserving devices for Card Management Pod")
		rDevs, err := ap.SelectDevicesForCardManagement(p, nodeState, nodeName)
		if err != nil {
			return nil, fmt.Errorf("failed to select devices for Card Management Pod: %w", err)
		}
		return rDevs, nil
	}

	isCardMgmtRunner := isCardManagementRunnerPod(p, spyrepol.Status.Namespace)
	klog.Info("checking remaining devices")
	nRemaining, err := ap.getNumRemainingDevices(ctx, nodeState, isCardMgmtRunner, rName)
	if err != nil {
		return nil, err
	}
	if nReq > int64(nRemaining) {
		return nil, fmt.Errorf("failed to reserve enough devices: num_requested: %d, num_remaining: %d", nReq, nRemaining)
	}

	klog.Info("choosing devices")
	devs, err := selectDevicesFromState(nodeState, isCardMgmtRunner, rName, nReq)
	if err != nil {
		klog.ErrorS(err, "failed to select device(s)",
			"resource_name", rName, "num_requested", nReq, "num_remaining", nRemaining)
		return nil, err
	}

	klog.Infof("devices selected: %v (node: %s, rName: %s, nReq: %v)", devs, nodeName, rName, nReq)
	return map[string][]string{rName: devs}, nil
}

// Unreserve deletes the devices which were reserved for the Pod from reservedSpyreInterfaces in SpyreNodeState.
//
// The scheduling framework calls Unreserve both when Reserve itself failed and
// when a later extension point rejected the Pod, so without it a Pod that
// cleared Reserve and then failed to bind would keep its devices reserved until
// some later scheduling cycle happened to prune them.
func (ap *SpyrePlugin) Unreserve(ctx context.Context, state framework.CycleState, p *corev1.Pod, nodeName string) {
	klog.Infof("Unreserve: %s/%s (node: %s)", p.Namespace, p.Name, nodeName)
	if err := ap.unreserveDevices(ctx, p, nodeName); err != nil {
		// Unreserve has no way to report failure and the scheduling cycle is
		// already being abandoned, so log and leave the reservation to be pruned
		// by the next cycle on this node.
		klog.ErrorS(err, "failed to release reservation", "pod", klog.KObj(p), "node", nodeName)
	}
}

func (ap *SpyrePlugin) unreserveDevices(ctx context.Context, p *corev1.Pod, nodeName string) error {
	_, err := ap.spyreClient.MutateNodeStateStatus(ctx, nodeName,
		func(nodeState *spyrev1alpha1.SpyreNodeState) error {
			if !nodeState.Status.ReleaseReservation(podReference(p)) {
				// Either Reserve never got as far as writing a reservation, or a
				// previous Unreserve already released it. Both are expected, and
				// neither needs a write - which is what makes Unreserve idempotent.
				klog.Infof("no reservation to release for %s/%s on node %s", p.Namespace, p.Name, nodeName)
				return spyreclient.ErrNoStatusChange
			}
			klog.Infof("reservation released, remaining: %v", nodeState.Status.Reservations)
			return nil
		})
	return err
}
