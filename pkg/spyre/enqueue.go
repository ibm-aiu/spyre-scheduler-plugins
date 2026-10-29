/*
 * +-------------------------------------------------------------------+
 * | Copyright (c) 2026 IBM Corp.                                      |
 * | SPDX-License-Identifier: Apache-2.0                               |
 * +-------------------------------------------------------------------+
 */

package spyre

import (
	"context"
	"fmt"
	"reflect"

	spyrev1alpha1 "github.com/ibm-aiu/spyre-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
)

// this file implements EnqueueExtensions interface of the scheduling framework.

var _ fwk.EnqueueExtensions = &SpyrePlugin{}

// Custom resources are registered as <plural>.<version>.<group>; the scheduler
// watches them with a dynamic informer.
const (
	nodeStateResource     fwk.EventResource = "spyrenodestates.v1alpha1.spyre.ibm.com"
	clusterPolicyResource fwk.EventResource = "spyreclusterpolicies.v1alpha1.spyre.ibm.com"
)

// EventsToRegister returns the events that can make a Pod this plugin rejected
// schedulable.
//
// Devices are released in SpyreNodeState, which the scheduler does not watch
// unless a plugin asks it to. Without this a Pod rejected for lack of devices
// was retried only when some unrelated Pod or Node event happened to arrive, or
// after the unschedulable queue's five-minute flush.
func (ap *SpyrePlugin) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		{
			Event:          fwk.ClusterEvent{Resource: nodeStateResource, ActionType: fwk.Add | fwk.Update},
			QueueingHintFn: isSchedulableAfterNodeStateChange,
		},
		// The policy decides whether user Pods may be placed at all, and on
		// which nodes they may take PFs.
		{Event: fwk.ClusterEvent{Resource: clusterPolicyResource, ActionType: fwk.Add | fwk.Update}},
		// A deleted Pod's reservation is released once the grace period has
		// passed, by whichever cycle next prunes the node.
		{
			Event:          fwk.ClusterEvent{Resource: fwk.Pod, ActionType: fwk.Delete},
			QueueingHintFn: isSchedulableAfterPodDeletion,
		},
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add}},
	}, nil
}

// isSchedulableAfterNodeStateChange queues the Pod when a node gains devices or
// a device stops being taken. Most updates do neither: a reservation being made,
// or turned into an allocation by the device plugin, only takes devices.
func isSchedulableAfterNodeStateChange(logger klog.Logger, pod *corev1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	newState, err := toNodeState(newObj)
	if err != nil {
		return fwk.Queue, err
	}
	if oldObj == nil {
		logger.V(4).Info("SpyreNodeState added", "pod", klog.KObj(pod), "node", newState.Spec.NodeName)
		return fwk.Queue, nil
	}
	oldState, err := toNodeState(oldObj)
	if err != nil {
		return fwk.Queue, err
	}
	if !reflect.DeepEqual(oldState.Spec, newState.Spec) {
		logger.V(4).Info("SpyreNodeState spec changed", "pod", klog.KObj(pod), "node", newState.Spec.NodeName)
		return fwk.Queue, nil
	}
	if released := takenDevices(oldState).Difference(takenDevices(newState)); released.Len() > 0 {
		logger.V(4).Info("devices released", "pod", klog.KObj(pod), "node", newState.Spec.NodeName,
			"devices", sets.List(released))
		return fwk.Queue, nil
	}
	return fwk.QueueSkip, nil
}

// takenDevices returns every device on the node that is allocated, reserved or
// unhealthy.
func takenDevices(nodeState *spyrev1alpha1.SpyreNodeState) sets.Set[string] {
	taken := sets.New(nodeState.Status.AllocatedDevices()...)
	taken.Insert(allReservedDevices(nodeState)...)
	for _, u := range nodeState.Status.UnhealthyDevices {
		taken.Insert(u.ID)
	}
	return taken
}

// toNodeState accepts both the typed object and the unstructured one the
// scheduler's dynamic informer delivers.
func toNodeState(obj interface{}) (*spyrev1alpha1.SpyreNodeState, error) {
	switch o := obj.(type) {
	case *spyrev1alpha1.SpyreNodeState:
		return o, nil
	case *unstructured.Unstructured:
		nodeState := &spyrev1alpha1.SpyreNodeState{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(o.UnstructuredContent(), nodeState); err != nil {
			return nil, fmt.Errorf("failed to convert %s to SpyreNodeState: %w", o.GetName(), err)
		}
		return nodeState, nil
	default:
		return nil, fmt.Errorf("unexpected object for SpyreNodeState event: %T", obj)
	}
}

// isSchedulableAfterPodDeletion queues the Pod only when the deleted Pod used
// Spyre devices; nothing else can be released by its deletion.
func isSchedulableAfterPodDeletion(logger klog.Logger, pod *corev1.Pod, oldObj, _ interface{}) (fwk.QueueingHint, error) {
	deleted, ok := oldObj.(*corev1.Pod)
	if !ok {
		return fwk.Queue, fmt.Errorf("unexpected object for Pod deletion: %T", oldObj)
	}
	m, err := getNumRequestedDevices(deleted)
	if err != nil || len(m) == 0 {
		return fwk.QueueSkip, nil
	}
	logger.V(4).Info("Pod holding Spyre devices deleted", "pod", klog.KObj(pod), "deletedPod", klog.KObj(deleted))
	return fwk.Queue, nil
}
