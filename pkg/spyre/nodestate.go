/*
 * +-------------------------------------------------------------------+
 * | Copyright (c) 2026 IBM Corp.                                      |
 * | SPDX-License-Identifier: Apache-2.0                               |
 * +-------------------------------------------------------------------+
 */

package spyre

import (
	"context"
	"errors"
	"fmt"
	"time"

	spyrev1alpha1 "github.com/ibm-aiu/spyre-operator/api/v1alpha1"
	spyreclient "github.com/ibm-aiu/spyre-operator/pkg/client"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/util/assumecache"
)

// nodeNameIndex indexes SpyreNodeStates by the node they describe. The object
// name is not guaranteed to be the node name, so lookups go by Spec.NodeName.
const nodeNameIndex = "spec.nodeName"

// nodeStateStore reads and writes SpyreNodeStates for the plugin.
//
// Reads are served from an informer, so finding the state of a node costs no
// API request. The informer lags behind the API, which is safe for writes
// because every write carries the resourceVersion of the state it was computed
// from: a write computed from a stale copy is rejected with a conflict and is
// recomputed from a fresh read. What the plugin wrote itself is "assumed" into
// the cache straight away, so the next scheduling cycle sees its own
// reservations without waiting for the watch to deliver them.
type nodeStateStore struct {
	spyreClient *spyreclient.SpyreClient

	// cache is nil when the plugin has no informer, which is the case in unit
	// tests. Every read then goes to the API.
	cache *assumecache.AssumeCache
	// synced reports whether cache has received the informer's initial list.
	// Until it has, reads go to the API.
	synced func() bool
}

// newNodeStateStore returns a store that reads through cache once it is synced.
// informer may be nil, in which case every read goes to the API.
func newNodeStateStore(logger klog.Logger, spyreClient *spyreclient.SpyreClient,
	informer assumecache.Informer) *nodeStateStore {

	s := &nodeStateStore{spyreClient: spyreClient}
	if informer == nil {
		return s
	}
	s.setCache(assumecache.NewAssumeCache(logger, informer, "SpyreNodeState", nodeNameIndex, nodeStateNodeName))
	return s
}

func (s *nodeStateStore) setCache(c *assumecache.AssumeCache) {
	s.cache = c
	// The cache hands back the registration of its own informer handler, so
	// this reports whether the cache - not just the informer - has caught up.
	s.synced = c.AddEventHandler(toolscache.ResourceEventHandlerFuncs{}).HasSynced
}

func nodeStateNodeName(obj interface{}) ([]string, error) {
	if obj == nil {
		// AddEventHandler lists the whole cache through the index with a nil
		// object; that lists nothing, which is all setCache needs.
		return nil, nil
	}
	nodeState, ok := obj.(*spyrev1alpha1.SpyreNodeState)
	if !ok {
		return nil, fmt.Errorf("unexpected object in SpyreNodeState cache: %T", obj)
	}
	return []string{nodeState.Spec.NodeName}, nil
}

// errNodeStateNotFound means the node has no SpyreNodeState.
var errNodeStateNotFound = errors.New("no SpyreNodeState resource")

// get returns a copy of the SpyreNodeState of nodeName, which the caller may
// modify. It may be slightly behind the API.
func (s *nodeStateStore) get(ctx context.Context, nodeName string) (*spyrev1alpha1.SpyreNodeState, error) {
	if s.cache == nil || !s.synced() {
		return s.spyreClient.GetSpyreNodeState(ctx, nodeName)
	}
	objs := s.cache.List(&spyrev1alpha1.SpyreNodeState{Spec: spyrev1alpha1.SpyreNodeStateSpec{NodeName: nodeName}})
	switch len(objs) {
	case 0:
		// The cache has seen the full list, so the node has no state. Asking
		// the API would only repeat the answer at the price of a LIST.
		return nil, fmt.Errorf("%w for node '%s'", errNodeStateNotFound, nodeName)
	case 1:
		return objs[0].(*spyrev1alpha1.SpyreNodeState).DeepCopy(), nil
	default:
		// Two objects claim the same node. Leave it to the API lookup to pick
		// one, as it did before there was a cache.
		klog.Warningf("%d SpyreNodeStates found for node %s", len(objs), nodeName)
		return s.spyreClient.GetSpyreNodeState(ctx, nodeName)
	}
}

// getLive reads the SpyreNodeState of nodeName from the API, by object name when
// it is known.
func (s *nodeStateStore) getLive(ctx context.Context, nodeName, objName string) (*spyrev1alpha1.SpyreNodeState, error) {
	if objName == "" {
		return s.spyreClient.GetSpyreNodeState(ctx, nodeName)
	}
	return s.spyreClient.Get(ctx, objName)
}

// errCachedStateRejected marks a mutation that failed against a cached copy of
// the node state. The cache may be missing a release made by someone else, so
// the mutation deserves another try against the API before it is reported.
var errCachedStateRejected = errors.New("mutation failed against cached SpyreNodeState")

// mutateStatus applies mutate to the SpyreNodeState of nodeName and writes the
// resulting status back, retrying the whole read-modify-write cycle when another
// writer gets there first.
//
// The first attempt starts from the cache. Every later attempt - after a
// conflict, or after mutate rejected the cached copy - starts from the API, so
// a stale cache costs at most one extra round trip.
//
// mutate may return spyreclient.ErrNoStatusChange to finish successfully
// without writing. Any other error from mutate is returned unchanged.
func (s *nodeStateStore) mutateStatus(ctx context.Context, nodeName string,
	mutate func(*spyrev1alpha1.SpyreNodeState) error) (*spyrev1alpha1.SpyreNodeState, error) {

	var result *spyrev1alpha1.SpyreNodeState
	var mutateErr error
	var objName string
	fromCache := s.cache != nil

	err := retry.OnError(nodeStateUpdateBackoff, isRetriableMutation, func() error {
		mutateErr = nil
		var nodeState *spyrev1alpha1.SpyreNodeState
		var err error
		if fromCache {
			nodeState, err = s.get(ctx, nodeName)
		} else {
			nodeState, err = s.getLive(ctx, nodeName, objName)
		}
		cached := fromCache
		fromCache = false
		if err != nil {
			if cached {
				// The object may be too new for the cache to have seen it.
				klog.V(4).Infof("SpyreNodeState of node %s not in cache, retrying against the API: %v", nodeName, err)
				return errCachedStateRejected
			}
			return fmt.Errorf("failed to get SpyreNodeState: %w", err)
		}
		objName = nodeState.Name

		nodeState.Status.NormalizeReservations()
		if err := mutate(nodeState); err != nil {
			if errors.Is(err, spyreclient.ErrNoStatusChange) {
				result = nodeState
				return nil
			}
			mutateErr = err
			if cached {
				klog.V(4).Infof("mutation of cached SpyreNodeState of node %s failed, retrying against the API: %v",
					nodeName, err)
				return errCachedStateRejected
			}
			return err
		}
		if _, err := s.spyreClient.UpdateStatus(ctx, nodeState, false); err != nil {
			// A conflict here is expected whenever another scheduling cycle or
			// the device plugin touched the node state, or the cache was behind;
			// the retry re-reads from the API and lets mutate recompute.
			if k8serr.IsConflict(err) {
				klog.Infof("SpyreNodeState status update conflicted for node %s, retrying: %v", nodeName, err)
			}
			return err
		}
		s.assume(nodeState)
		result = nodeState
		return nil
	})
	if err != nil {
		if mutateErr != nil {
			return nil, mutateErr
		}
		return nil, fmt.Errorf("failed to mutate SpyreNodeState status of node %s: %w", nodeName, err)
	}
	return result, nil
}

func isRetriableMutation(err error) bool {
	return k8serr.IsConflict(err) || errors.Is(err, errCachedStateRejected)
}

// assume records a state the API server has just accepted, so that reads see
// it before the informer delivers it.
func (s *nodeStateStore) assume(nodeState *spyrev1alpha1.SpyreNodeState) {
	if s.cache == nil {
		return
	}
	if err := s.cache.Assume(nodeState.DeepCopy()); err != nil {
		// The informer already delivered something newer, which is just as good.
		klog.V(4).Infof("not assuming SpyreNodeState %s: %v", nodeState.Name, err)
	}
}

// nodeStateUpdateBackoff gives the read-modify-write cycle more attempts than
// retry.DefaultRetry's five. A node that hosts a runner set sees a new single-card
// Pod every second or two, so several scheduling cycles and the device plugin can
// all be writing the same SpyreNodeState; losing a few races is normal rather
// than a sign of trouble. The backoff still tops out well under a second in total.
var nodeStateUpdateBackoff = wait.Backoff{
	Steps:    10,
	Duration: 10 * time.Millisecond,
	Factor:   1.4,
	Jitter:   0.2,
}
