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

	spyrev1alpha1 "github.com/ibm-aiu/spyre-operator/api/v1alpha1"
	spyreclient "github.com/ibm-aiu/spyre-operator/pkg/client"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// spyreClusterPolicyName is the name of the one SpyreClusterPolicy the operator
// creates.
const spyreClusterPolicyName = "spyreclusterpolicy"

// clusterPolicyStore reads the SpyreClusterPolicy for the plugin: from an
// informer once it has synced, and from the API until then.
type clusterPolicyStore struct {
	spyreClient *spyreclient.SpyreClient

	// cache is nil when the plugin has no informer, which is the case in unit
	// tests. Every read then goes to the API.
	cache client.Reader
	// synced reports whether the informer behind cache has its initial list.
	// Reading cache before that would block until it has.
	synced func() bool
}

func (s *clusterPolicyStore) get(ctx context.Context) (*spyrev1alpha1.SpyreClusterPolicy, error) {
	if s.cache != nil && s.synced() {
		pol := &spyrev1alpha1.SpyreClusterPolicy{}
		err := s.cache.Get(ctx, client.ObjectKey{Name: spyreClusterPolicyName}, pol)
		if err == nil {
			return pol, nil
		}
		if k8serr.IsNotFound(err) {
			// The cache has seen the full list, so the API would say the same.
			return nil, fmt.Errorf("failed to get spyre cluster policy: %w", err)
		}
		klog.V(4).Infof("failed to read SpyreClusterPolicy from cache, asking the API: %v", err)
	}
	return s.spyreClient.GetSpyreClusterPolicy(ctx, spyreClusterPolicyName)
}
