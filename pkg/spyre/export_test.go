/*
 * +-------------------------------------------------------------------+
 * | Copyright (c) 2025, 2026 IBM Corp.                                |
 * | SPDX-License-Identifier: Apache-2.0                               |
 * +-------------------------------------------------------------------+
 */

package spyre

import (
	"time"

	spyreclient "github.com/ibm-aiu/spyre-operator/pkg/client"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/util/assumecache"
)

var IsAvailable = isAvailable
var GetNumRequestedDevices = getNumRequestedDevices
var GetNumRemainingDevices = (*SpyrePlugin).getNumRemainingDevices
var ReserveDevices = (*SpyrePlugin).reserveDevices
var UnreserveDevices = (*SpyrePlugin).unreserveDevices
var PodReference = podReference
var DeviceTree = deviceTree
var PruneStaleReservations = (*SpyrePlugin).pruneStaleReservations

// ExportSetReservationGracePeriod sets the grace period and returns the previous
// value, so a test can decide whether a reservation whose Pod has vanished is
// released at once or held on to.
func ExportSetReservationGracePeriod(d time.Duration) time.Duration {
	previous := reservationGracePeriod
	reservationGracePeriod = d
	return previous
}

var ExportFilter = (*SpyrePlugin).filter
var SelectDevices = (*SpyrePlugin).SelectDevices
var SelectDevicesForCardManagement = (*SpyrePlugin).SelectDevicesForCardManagement
var IsCardManagementPod = isCardManagementPod
var IsCardManagementRunnerPod = isCardManagementRunnerPod
var HasSSADevice = hasSSADevice

// ExportSetSpyreClient makes the plugin use ac, reading every SpyreNodeState and
// SpyreClusterPolicy straight from the API.
func (ap *SpyrePlugin) ExportSetSpyreClient(ac *spyreclient.SpyreClient) {
	ap.spyreClient = ac
	ap.nodeStates = newNodeStateStore(klog.Background(), ac, nil)
	ap.policies = &clusterPolicyStore{spyreClient: ac}
}

// ExportSetNodeStateCache makes the plugin read SpyreNodeStates from c, which
// is treated as synced.
func (ap *SpyrePlugin) ExportSetNodeStateCache(c *assumecache.AssumeCache) {
	ap.nodeStates.setCache(c)
}

// ExportNewNodeStateCache returns an empty SpyreNodeState cache without an
// informer behind it; the test fills it with assumecache.AddTestObject.
func ExportNewNodeStateCache() *assumecache.AssumeCache {
	return assumecache.NewAssumeCache(klog.Background(), nil, "SpyreNodeState", nodeNameIndex, nodeStateNodeName)
}
func (ap *SpyrePlugin) ExportSetPodLister(pl corelisters.PodLister) {
	ap.podLister = pl
}
