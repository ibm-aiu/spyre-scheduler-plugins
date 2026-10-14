/*
 * +-------------------------------------------------------------------+
 * | Copyright (c) 2026 IBM Corp.                                      |
 * | SPDX-License-Identifier: Apache-2.0                               |
 * +-------------------------------------------------------------------+
 */

package spyre_test

import (
	"context"
	"slices"

	spyrev1alpha1 "github.com/ibm-aiu/spyre-operator/api/v1alpha1"
	spyreconst "github.com/ibm-aiu/spyre-operator/const"
	spyreclient "github.com/ibm-aiu/spyre-operator/pkg/client"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/scheduler/util/assumecache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/scheduler-plugins/pkg/spyre"
)

var _ = Describe("SpyreNodeState cache", Ordered, func() {

	ctx := context.Background()
	var ap *spyre.SpyrePlugin
	var nodeStateCache *assumecache.AssumeCache
	var appNs string
	var holder *corev1.Pod

	BeforeEach(func() {
		var err error
		spyreClient, err = spyreclient.NewClient(ctx, cfg)
		Expect(err).To(BeNil())
		k8sClient, err = client.New(cfg, client.Options{})
		Expect(err).NotTo(HaveOccurred())

		appNs = createNewNamespace(ctx, k8sClient)
		holder = buildPod("holder", appNs, spyreconst.PfResourceName, 1)
		Expect(k8sClient.Create(ctx, holder)).To(Succeed())
		prepareSpyreNodeState([]string{}, [][]string{{"0000:b9:00.0"}}, []spyrev1alpha1.Pod{spyre.PodReference(holder)})

		pol := &spyrev1alpha1.SpyreClusterPolicy{ObjectMeta: metav1.ObjectMeta{Name: spyrepolName}}
		pol, err = spyreClient.CreateSpyreClusterPolicy(ctx, pol)
		Expect(err).To(BeNil())
		pol.Status.Namespace = "aiu-operator"
		pol.Status.State = spyrev1alpha1.Ready
		_, err = spyreClient.UpdateSpyreClusterPolicyStatus(ctx, pol, true)
		Expect(err).To(BeNil())

		ap = &spyre.SpyrePlugin{}
		ap.ExportSetSpyreClient(spyreClient)
		ap.ExportSetPodLister(clientPodLister{k8sClient})
		nodeStateCache = spyre.ExportNewNodeStateCache()
		ap.ExportSetNodeStateCache(nodeStateCache)
	})

	AfterEach(func() {
		Expect(spyreClient.DeleteAll(ctx)).To(Succeed())
		Expect(spyreClient.DeleteSpyreClusterPolicy(ctx, spyrepolName, &client.DeleteOptions{})).To(Succeed())
	})

	// cacheNodeState puts the current API state of node1 into the cache, after
	// applying edit to it when edit is not nil.
	cacheNodeState := func(edit func(*spyrev1alpha1.SpyreNodeState)) {
		s, err := spyreClient.GetSpyreNodeState(ctx, nodeName)
		Expect(err).To(BeNil())
		if edit != nil {
			edit(s)
		}
		assumecache.AddTestObject(nodeStateCache, s)
	}

	// reserveEverything reserves every PF of s for the holder Pod.
	reserveEverything := func(s *spyrev1alpha1.SpyreNodeState) {
		var all []string
		for _, sIf := range s.Spec.SpyreInterfaces {
			all = append(all, sIf.PciAddress)
		}
		s.Status.NormalizeReservations()
		s.Status.ReserveDevices(spyreconst.PfResourceName, spyre.PodReference(holder), all, metav1.Now())
	}

	// apiNodeState returns the state of node1 as the API has it.
	apiNodeState := func() *spyrev1alpha1.SpyreNodeState {
		s, err := spyreClient.GetSpyreNodeState(ctx, nodeName)
		Expect(err).To(BeNil())
		s.Status.NormalizeReservations()
		return s
	}

	expectNoDoubleBooking := func(s *spyrev1alpha1.SpyreNodeState) {
		all := s.Status.AllocatedDevices()
		all = append(all, s.Status.ReservedDevices()...)
		slices.Sort(all)
		Expect(slices.Compact(slices.Clone(all))).Should(HaveLen(len(all)), "double booking: %v", all)
	}

	It("serves reads from the cache", func() {
		// The cache claims every device is taken; the API has eight free. A
		// read that went to the API would find room.
		cacheNodeState(reserveEverything)

		_, _, err := spyre.SelectDevices(ap, ctx, false, spyreconst.PfResourceName, 1, nodeName)
		Expect(err).To(HaveOccurred())
	})

	It("does not hand out a device the cache does not know is taken", func() {
		// The cache holds the state from before another Pod reserved seven of
		// the eight free devices, so it offers devices that are already gone.
		cacheNodeState(nil)

		other := buildPod("other", appNs, spyreconst.PfResourceName, 7)
		Expect(k8sClient.Create(ctx, other)).To(Succeed())
		direct := &spyre.SpyrePlugin{}
		direct.ExportSetSpyreClient(spyreClient)
		direct.ExportSetPodLister(clientPodLister{k8sClient})
		Expect(spyre.ReserveDevices(direct, ctx, other, nodeName)).To(Succeed())

		mine := buildPod("mine", appNs, spyreconst.PfResourceName, 1)
		Expect(k8sClient.Create(ctx, mine)).To(Succeed())
		Expect(spyre.ReserveDevices(ap, ctx, mine, nodeName)).To(Succeed())

		s := apiNodeState()
		expectNoDoubleBooking(s)
		e, found := s.Status.Reservations[spyreconst.PfResourceName].EntryForPod(spyre.PodReference(mine))
		Expect(found).Should(BeTrue())
		Expect(e.DeviceList).Should(HaveLen(1))
	})

	It("retries against the API when the cached state has no room", func() {
		// The cache still shows every device reserved, but the API has
		// released them. Failing on the cached copy alone would turn a cache
		// that is a moment behind into a failed scheduling attempt.
		cacheNodeState(reserveEverything)

		mine := buildPod("mine", appNs, spyreconst.PfResourceName, 1)
		Expect(k8sClient.Create(ctx, mine)).To(Succeed())
		Expect(spyre.ReserveDevices(ap, ctx, mine, nodeName)).To(Succeed())
		expectNoDoubleBooking(apiNodeState())
	})

	It("sees its own reservation before the informer delivers it", func() {
		cacheNodeState(nil)

		first := buildPod("first", appNs, spyreconst.PfResourceName, 4)
		Expect(k8sClient.Create(ctx, first)).To(Succeed())
		Expect(spyre.ReserveDevices(ap, ctx, first, nodeName)).To(Succeed())

		// Nothing feeds the cache but the plugin itself, so the reservation is
		// visible only if the write was assumed into it.
		cached, err := nodeStateCache.Get(nodeName)
		Expect(err).To(BeNil())
		s := cached.(*spyrev1alpha1.SpyreNodeState)
		Expect(s.ResourceVersion).Should(Equal(apiNodeState().ResourceVersion))
		_, found := s.Status.Reservations[spyreconst.PfResourceName].EntryForPod(spyre.PodReference(first))
		Expect(found).Should(BeTrue())

		// A second Pod reserved from that cache must not take the first Pod's
		// devices.
		second := buildPod("second", appNs, spyreconst.PfResourceName, 4)
		Expect(k8sClient.Create(ctx, second)).To(Succeed())
		Expect(spyre.ReserveDevices(ap, ctx, second, nodeName)).To(Succeed())
		expectNoDoubleBooking(apiNodeState())
	})
})
