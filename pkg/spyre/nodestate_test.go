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
	fwk "k8s.io/kube-scheduler/framework"
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

		pol := &spyrev1alpha1.SpyreClusterPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: spyrepolName},
			Spec:       spyrev1alpha1.SpyreClusterPolicySpec{DevicePlugin: spyrev1alpha1.DevicePluginSpec{Mode: spyrev1alpha1.DevicePluginModeClassic}},
		}
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

	It("takes the word of the API over a cache that has no room in Filter", func() {
		// The cache claims every device is taken but the API has eight free:
		// the release has not reached the cache yet. Rejecting the Pod would
		// park it until the periodic flush, because the update that carried the
		// release has already been judged and is not coming again.
		cacheNodeState(reserveEverything)
		before := apiNodeState()

		mine := buildPod("mine", appNs, spyreconst.PfResourceName, 1)
		status := spyre.ExportFilter(ap, ctx, mine, nodeName)
		Expect(status.IsSuccess()).Should(BeTrue(), status.Message())

		// Filter read from the API but did not write to it.
		Expect(apiNodeState().ResourceVersion).Should(Equal(before.ResourceVersion))
		// The cache has caught up with the API.
		cached, err := nodeStateCache.Get(nodeName)
		Expect(err).To(BeNil())
		cachedState := cached.(*spyrev1alpha1.SpyreNodeState).DeepCopy()
		cachedState.Status.NormalizeReservations()
		Expect(cachedState.Status.ReservedDevices()).Should(ConsistOf(before.Status.ReservedDevices()))
		Expect(cachedState.Status.ReservedDevices()).ShouldNot(HaveLen(len(cachedState.Spec.SpyreInterfaces)))
	})

	It("still rejects in Filter when the API has no room either", func() {
		s := apiNodeState()
		reserveEverything(s)
		_, err := spyreClient.UpdateStatus(ctx, s, false)
		Expect(err).To(BeNil())
		cacheNodeState(nil)
		before := apiNodeState()

		mine := buildPod("mine", appNs, spyreconst.PfResourceName, 1)
		status := spyre.ExportFilter(ap, ctx, mine, nodeName)
		Expect(status.Code()).Should(Equal(fwk.UnschedulableAndUnresolvable), status.Message())
		Expect(status.Message()).Should(Equal("not enough devices remain (spyre_pf): 1 requested"))
		Expect(apiNodeState().ResourceVersion).Should(Equal(before.ResourceVersion))
	})

	It("leaves the node state alone in Filter", func() {
		// The holder is being scheduled again, so its earlier reservation is
		// stale. Filter must judge the node without it, but dropping it for
		// real is Reserve's job: a write per node per Pod is what made every
		// scheduling cycle collide with the others.
		cacheNodeState(nil)
		before := apiNodeState()
		status := spyre.ExportFilter(ap, ctx, holder, nodeName)
		Expect(status.IsSuccess()).Should(BeTrue(), status.Message())
		after := apiNodeState()
		Expect(after.ResourceVersion).Should(Equal(before.ResourceVersion))
		_, found := after.Status.Reservations[spyreconst.PfResourceName].EntryForPod(spyre.PodReference(holder))
		Expect(found).Should(BeTrue())
	})

	It("gives the same reason for a shortage however many devices are free", func() {
		// The scheduler patches the Pod each time the reason changes, so a
		// reason that said how many devices were free would cost a write on
		// nearly every attempt.
		greedy := buildPod("greedy", appNs, spyreconst.PfResourceName, 9)
		reasonWith := func(edit func(*spyrev1alpha1.SpyreNodeState)) string {
			nodeStateCache = spyre.ExportNewNodeStateCache()
			ap.ExportSetNodeStateCache(nodeStateCache)
			cacheNodeState(edit)
			status := spyre.ExportFilter(ap, ctx, greedy, nodeName)
			Expect(status.Code()).Should(Equal(fwk.UnschedulableAndUnresolvable), status.Message())
			return status.Message()
		}
		eightFree := reasonWith(nil)
		fourFree := reasonWith(func(s *spyrev1alpha1.SpyreNodeState) {
			s.Status.NormalizeReservations()
			s.Status.ReserveDevices(spyreconst.PfResourceName, spyre.PodReference(holder),
				[]string{"0000:1c:00.0", "0000:1f:00.0", "0000:20:00.0", "0000:36:00.0"}, metav1.Now())
		})
		Expect(fourFree).Should(Equal(eightFree))
		Expect(eightFree).Should(Equal("not enough devices remain (spyre_pf): 9 requested"))
	})

	It("rejects only the node when the node has no state", func() {
		// An error status would abort the scheduling of the Pod on every node.
		cacheNodeState(nil)
		status := spyre.ExportFilter(ap, ctx, holder, "node-without-state")
		Expect(status.Code()).Should(Equal(fwk.UnschedulableAndUnresolvable), status.Message())
	})

	DescribeTable("filters user Pods by the state of the cluster policy",
		func(state spyrev1alpha1.State, schedulable bool) {
			cacheNodeState(nil)
			pol, err := spyreClient.GetSpyreClusterPolicy(ctx, spyrepolName)
			Expect(err).To(BeNil())
			pol.Status.State = state
			_, err = spyreClient.UpdateSpyreClusterPolicyStatus(ctx, pol, true)
			Expect(err).To(BeNil())

			status := spyre.ExportFilter(ap, ctx, holder, nodeName)
			if schedulable {
				Expect(status.IsSuccess()).Should(BeTrue(), status.Message())
				return
			}
			// The Pod waits for the policy to change rather than cycling
			// through the backoff queue.
			Expect(status.Code()).Should(Equal(fwk.UnschedulableAndUnresolvable), status.Message())
		},
		Entry("ready", spyrev1alpha1.Ready, true),
		Entry("degraded", spyrev1alpha1.Degraded, true),
		Entry("not ready", spyrev1alpha1.NotReady, false),
		Entry("ignored", spyrev1alpha1.Ignored, false),
		Entry("no Spyre nodes", spyrev1alpha1.NoSpyreNodes, false),
		Entry("no node feature discovery", spyrev1alpha1.NoNFD, false),
	)

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
