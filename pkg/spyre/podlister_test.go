/*
 * +-------------------------------------------------------------------+
 * | Copyright (c) 2026 IBM Corp.                                      |
 * | SPDX-License-Identifier: Apache-2.0                               |
 * +-------------------------------------------------------------------+
 */

package spyre_test

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	corelisters "k8s.io/client-go/listers/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// clientPodLister is a PodLister that reads straight from the API server.
//
// The plugin reads the scheduler's Pod informer. The tests create a Pod and
// expect the very next call to see it, which a live read guarantees and an
// informer does not.
type clientPodLister struct {
	c client.Client
}

var _ corelisters.PodLister = clientPodLister{}

func (l clientPodLister) List(selector labels.Selector) ([]*corev1.Pod, error) {
	return listPods(l.c, selector)
}

func (l clientPodLister) Pods(namespace string) corelisters.PodNamespaceLister {
	return clientPodNamespaceLister{c: l.c, namespace: namespace}
}

type clientPodNamespaceLister struct {
	c         client.Client
	namespace string
}

func (l clientPodNamespaceLister) List(selector labels.Selector) ([]*corev1.Pod, error) {
	return listPods(l.c, selector, client.InNamespace(l.namespace))
}

func (l clientPodNamespaceLister) Get(name string) (*corev1.Pod, error) {
	pod := &corev1.Pod{}
	key := client.ObjectKey{Namespace: l.namespace, Name: name}
	if err := l.c.Get(context.Background(), key, pod); err != nil {
		return nil, err
	}
	return pod, nil
}

func listPods(c client.Client, selector labels.Selector, opts ...client.ListOption) ([]*corev1.Pod, error) {
	list := &corev1.PodList{}
	opts = append(opts, client.MatchingLabelsSelector{Selector: selector})
	if err := c.List(context.Background(), list, opts...); err != nil {
		return nil, err
	}
	pods := make([]*corev1.Pod, 0, len(list.Items))
	for i := range list.Items {
		pods = append(pods, &list.Items[i])
	}
	return pods, nil
}
