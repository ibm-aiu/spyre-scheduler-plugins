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
		return nil, fmt.Errorf("failed to get SpyreNodeState resource for node '%s'", nodeName)
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

	m := &mutation{store: s, nodeName: nodeName, mutate: mutate, fromCache: s.cache != nil}
	if err := retry.OnError(nodeStateUpdateBackoff, isRetriableMutation, func() error { return m.attempt(ctx) }); err != nil {
		if m.mutateErr != nil {
			return nil, m.mutateErr
		}
		return nil, fmt.Errorf("failed to mutate SpyreNodeState status of node %s: %w", nodeName, err)
	}
	return m.result, nil
}

// mutation is one mutateStatus call: the attempts it makes share where the next
// one reads from, and what the last one produced.
type mutation struct {
	store    *nodeStateStore
	nodeName string
	mutate   func(*spyrev1alpha1.SpyreNodeState) error

	// fromCache is true until the first attempt has been made.
	fromCache bool
	// objName is the object name found by the first read, which later reads use.
	objName string

	// result is the state that was written, or read when nothing needed writing.
	result *spyrev1alpha1.SpyreNodeState
	// mutateErr is the error mutate returned in the last attempt, if any.
	mutateErr error
}

// attempt makes one read-modify-write cycle. An error that isRetriableMutation
// accepts makes the caller try again.
func (m *mutation) attempt(ctx context.Context) error {
	m.mutateErr = nil
	cached := m.fromCache
	m.fromCache = false

	nodeState, err := m.read(ctx, cached)
	if err != nil {
		return err
	}
	m.objName = nodeState.Name

	nodeState.Status.NormalizeReservations()
	if err := m.mutate(nodeState); err != nil {
		return m.mutationFailed(nodeState, err, cached)
	}
	if err := m.write(ctx, nodeState); err != nil {
		return err
	}
	m.result = nodeState
	return nil
}

// read returns the state to mutate: from the cache on the first attempt, from
// the API on the later ones.
func (m *mutation) read(ctx context.Context, cached bool) (*spyrev1alpha1.SpyreNodeState, error) {
	if !cached {
		nodeState, err := m.store.getLive(ctx, m.nodeName, m.objName)
		if err != nil {
			return nil, fmt.Errorf("failed to get SpyreNodeState: %w", err)
		}
		return nodeState, nil
	}
	nodeState, err := m.store.get(ctx, m.nodeName)
	if err != nil {
		// The object may be too new for the cache to have seen it.
		klog.V(4).Infof("SpyreNodeState of node %s not in cache, retrying against the API: %v", m.nodeName, err)
		return nil, errCachedStateRejected
	}
	return nodeState, nil
}

// mutationFailed turns the error mutate returned into the result of the attempt.
func (m *mutation) mutationFailed(nodeState *spyrev1alpha1.SpyreNodeState, err error, cached bool) error {
	if errors.Is(err, spyreclient.ErrNoStatusChange) {
		m.result = nodeState
		return nil
	}
	m.mutateErr = err
	if cached {
		klog.V(4).Infof("mutation of cached SpyreNodeState of node %s failed, retrying against the API: %v",
			m.nodeName, err)
		return errCachedStateRejected
	}
	return err
}

// write stores the mutated status and records it in the cache.
func (m *mutation) write(ctx context.Context, nodeState *spyrev1alpha1.SpyreNodeState) error {
	if _, err := m.store.spyreClient.UpdateStatus(ctx, nodeState, false); err != nil {
		// A conflict here is expected whenever another scheduling cycle or
		// the device plugin touched the node state, or the cache was behind;
		// the retry re-reads from the API and lets mutate recompute.
		if k8serr.IsConflict(err) {
			klog.V(4).Infof("SpyreNodeState status update conflicted for node %s, retrying: %v", m.nodeName, err)
		}
		return err
	}
	m.store.assume(nodeState)
	return nil
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
