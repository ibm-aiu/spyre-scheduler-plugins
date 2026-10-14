/*
 * +-------------------------------------------------------------------+
 * | Copyright (c) 2026 IBM Corp.                                      |
 * | SPDX-License-Identifier: Apache-2.0                               |
 * +-------------------------------------------------------------------+
 */

package spyre_test

import (
	"context"

	spyrev1alpha1 "github.com/ibm-aiu/spyre-operator/api/v1alpha1"
	spyreconst "github.com/ibm-aiu/spyre-operator/const"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"sigs.k8s.io/scheduler-plugins/pkg/spyre"
)

var _ = Describe("Queueing hints", func() {

	logger := klog.Background()
	pending := buildPod("pending", "app", spyreconst.PfResourceName, 1)
	holder := spyrev1alpha1.Pod{Namespace: "app", Name: "holder", UID: "uid-holder"}

	baseState := func() *spyrev1alpha1.SpyreNodeState {
		return &spyrev1alpha1.SpyreNodeState{
			ObjectMeta: metav1.ObjectMeta{Name: "node1"},
			Spec: spyrev1alpha1.SpyreNodeStateSpec{
				NodeName: "node1",
				SpyreInterfaces: []spyrev1alpha1.SpyreInterface{
					{PciAddress: "pf1"}, {PciAddress: "pf2"}, {PciAddress: "pf3"},
				},
			},
			Status: spyrev1alpha1.SpyreNodeStateStatus{
				AllocationList: []spyrev1alpha1.Allocation{{
					DeviceList: []string{"pf1"}, Pod: &spyrev1alpha1.Pod{Namespace: "app", Name: "running"},
					ResourcePool: spyreconst.PfResourceName,
				}},
			},
		}
	}

	hint := func(oldObj, newObj interface{}) fwk.QueueingHint {
		h, err := spyre.IsSchedulableAfterNodeStateChange(logger, pending, oldObj, newObj)
		Expect(err).To(BeNil())
		return h
	}

	It("registers SpyreNodeState under the name the scheduler watches custom resources by", func() {
		events, err := (&spyre.SpyrePlugin{}).EventsToRegister(context.Background())
		Expect(err).To(BeNil())
		var resources []fwk.EventResource
		for _, e := range events {
			resources = append(resources, e.Event.Resource)
		}
		Expect(resources).Should(ContainElements(
			fwk.EventResource("spyrenodestates.v1alpha1.spyre.ibm.com"),
			fwk.EventResource("spyreclusterpolicies.v1alpha1.spyre.ibm.com"),
		))
	})

	It("queues on a new node state", func() {
		Expect(hint(nil, baseState())).Should(Equal(fwk.Queue))
	})

	It("skips a reservation being made", func() {
		newState := baseState()
		newState.Status.ReserveDevices(spyreconst.PfResourceName, holder, []string{"pf2"}, metav1.Now())
		Expect(hint(baseState(), newState)).Should(Equal(fwk.QueueSkip))
	})

	It("skips a reservation being turned into an allocation", func() {
		oldState := baseState()
		oldState.Status.ReserveDevices(spyreconst.PfResourceName, holder, []string{"pf2"}, metav1.Now())
		newState := baseState()
		newState.Status.AllocationList = append(newState.Status.AllocationList, spyrev1alpha1.Allocation{
			DeviceList: []string{"pf2"}, Pod: &holder, ResourcePool: spyreconst.PfResourceName,
		})
		Expect(hint(oldState, newState)).Should(Equal(fwk.QueueSkip))
	})

	It("queues when an allocation is removed", func() {
		newState := baseState()
		newState.Status.AllocationList = nil
		Expect(hint(baseState(), newState)).Should(Equal(fwk.Queue))
	})

	It("queues when a reservation is released", func() {
		oldState := baseState()
		oldState.Status.ReserveDevices(spyreconst.PfResourceName, holder, []string{"pf2"}, metav1.Now())
		Expect(hint(oldState, baseState())).Should(Equal(fwk.Queue))
	})

	It("queues when a device recovers", func() {
		oldState := baseState()
		oldState.Status.UnhealthyDevices = []spyrev1alpha1.UnhealthyDevice{{ID: "pf3", State: "IN_ERROR"}}
		Expect(hint(oldState, baseState())).Should(Equal(fwk.Queue))
	})

	It("queues when the devices of the node change", func() {
		newState := baseState()
		newState.Spec.SpyreInterfaces = append(newState.Spec.SpyreInterfaces, spyrev1alpha1.SpyreInterface{PciAddress: "pf4"})
		Expect(hint(baseState(), newState)).Should(Equal(fwk.Queue))
	})

	It("understands the unstructured objects of the dynamic informer", func() {
		toUnstructured := func(s *spyrev1alpha1.SpyreNodeState) *unstructured.Unstructured {
			content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(s)
			Expect(err).To(BeNil())
			return &unstructured.Unstructured{Object: content}
		}
		newState := baseState()
		newState.Status.ReserveDevices(spyreconst.PfResourceName, holder, []string{"pf2"}, metav1.Now())
		Expect(hint(toUnstructured(baseState()), toUnstructured(newState))).Should(Equal(fwk.QueueSkip))
		Expect(hint(toUnstructured(newState), toUnstructured(baseState()))).Should(Equal(fwk.Queue))
	})

	It("queues on the deletion of a Pod that used Spyre devices only", func() {
		h, err := spyre.IsSchedulableAfterPodDeletion(logger, pending,
			buildPod("gone", "app", spyreconst.PfResourceName, 1), nil)
		Expect(err).To(BeNil())
		Expect(h).Should(Equal(fwk.Queue))

		plain := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "plain", Namespace: "app"},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "image"}}}}
		h, err = spyre.IsSchedulableAfterPodDeletion(logger, pending, plain, nil)
		Expect(err).To(BeNil())
		Expect(h).Should(Equal(fwk.QueueSkip))
	})
})
