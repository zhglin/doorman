// Copyright 2016 Google, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package doorman

import "time"

// Lease represents a lease on capacity for some resource for some client.
type Lease struct {
	// Expiry is the time at which this lease expires.
	// 租约到期的时间。
	Expiry time.Time

	// RefreshInterval is the interval at which the client should
	// poll for updates for this resource.
	// 是客户机应轮询此资源的更新的时间间隔。
	RefreshInterval time.Duration

	// Has is how much capacity was given to this client.
	// 是给这个客户端多少容量。
	Has float64

	// Wants is how much capacity was requested.
	// 是请求的想要的容量。
	Wants float64

	// Subclients is the number of subclients of this client.
	Subclients int64
}

// ClientLeaseStatus contains information about a lease that
// a client has.
type ClientLeaseStatus struct {
	ClientID string
	Lease    Lease
}

// ResourceLeaseStatus is a read-only copy of information about all leases
// for a particular resource. It can be used for display purposes.
type ResourceLeaseStatus struct {
	// Resource id.
	ID string

	// The sum of all outstanding obligations.
	SumHas float64

	// The sum of all client wants.
	SumWants float64

	// A slice containing information about outstanding leases
	Leases []ClientLeaseStatus
}

// IsZero returns true if the lease is a zero instance.
func (lease Lease) IsZero() bool {
	return lease.Expiry.IsZero()
}

// LeaseStore is the set of outstanding leases for some particular
// resource.
type LeaseStore interface {
	// SumHas returns the total capacity given out to clients
	// at the moment.
	SumHas() float64

	// SumWants returns the total capacity requested by clients.
	SumWants() float64

	// Get returns the lease currently granted to this client.
	Get(client string) Lease

	// HasClient returns whether this client currently has a lease.
	HasClient(client string) bool

	// Assign updates the lease store to represent capacity given
	// to a client.
	Assign(client string, leaseLength, refreshInterval time.Duration, has, wants float64, subclients int64) Lease

	// Release releases any leases granted to a client.
	Release(client string)

	// Clean removes any out of date leases. Returns the number of leases cleaned
	Clean() int

	// Count returns the number of subclients leases in the store.
	Count() int64

	// Returns a read-only copy of summary information about outstanding leases.
	ResourceLeaseStatus() ResourceLeaseStatus

	// Map executes a function for every lease in the store.
	Map(func(id string, lease Lease))

	// Subclients returns the number of subclients of the client with the given id.
	Subclients(id string) int64
}

type leaseStoreImpl struct {
	id       string
	leases   map[string]Lease
	sumWants float64
	sumHas   float64
	count    int64
}

// New returns a fresh LeaseStore implementation.
func NewLeaseStore(id string) LeaseStore {
	return &leaseStoreImpl{
		id:     id,
		leases: make(map[string]Lease),
	}
}

// Count 返回存储中子客户端租期的数量。
func (store *leaseStoreImpl) Count() int64 {
	return store.count
}

func (store *leaseStoreImpl) SumWants() float64 {
	return store.sumWants
}

// SumHas 将返回目前提供给客户的总容量。
func (store *leaseStoreImpl) SumHas() float64 {
	return store.sumHas
}

func (store *leaseStoreImpl) HasClient(client string) bool {
	_, ok := store.leases[client]
	return ok
}

// Get 返回当前授予此客户端的租约。
func (store *leaseStoreImpl) Get(client string) Lease {
	return store.leases[client]
}

// Release 释放客户端申请的资源容量
func (store *leaseStoreImpl) Release(client string) {
	lease, ok := store.leases[client]
	if !ok {
		return
	}
	store.sumWants -= lease.Wants
	store.sumHas -= lease.Has
	store.count -= lease.Subclients
	delete(store.leases, client)
}

// Assign 更新租赁存储以表示提供给客户端的容量。
// client 客户端标识
// leaseLength 租约时长
// refreshInterval 客户端刷新间隔
func (store *leaseStoreImpl) Assign(client string, leaseLength, refreshInterval time.Duration, has, wants float64, subclients int64) Lease {
	lease := store.leases[client]

	// 更新最新的统计值
	store.sumHas += has - lease.Has
	store.sumWants += wants - lease.Wants
	store.count += subclients - lease.Subclients

	// 更新租约信息
	lease.Has, lease.Wants = has, wants
	lease.Expiry = time.Now().Add(leaseLength)
	lease.RefreshInterval = refreshInterval
	lease.Subclients = subclients
	store.leases[client] = lease

	return lease
}

// Clean 删除任何过期租约。返回已清除租约的数量
func (store *leaseStoreImpl) Clean() int {
	when := time.Now()
	result := 0

	for client, lease := range store.leases {
		if when.After(lease.Expiry) {
			store.Release(client)
			result++
		}
	}

	return result
}

// ResourceLeaseStatus returns a read-only copy with summary information about outstanding leases.
func (store *leaseStoreImpl) ResourceLeaseStatus() ResourceLeaseStatus {
	status := ResourceLeaseStatus{
		ID:       store.id,
		SumHas:   store.sumHas,
		SumWants: store.sumWants,
		Leases:   make([]ClientLeaseStatus, 0, len(store.leases)),
	}

	for client, lease := range store.leases {
		status.Leases = append(status.Leases, ClientLeaseStatus{
			ClientID: client,
			Lease:    lease,
		})
	}

	return status
}

// Map executes a function for every lease in the store.
func (store *leaseStoreImpl) Map(fun func(string, Lease)) {
	for id, lease := range store.leases {
		fun(id, lease)
	}
}

// Subclients returns the number of subclients of the client with the given id.
// Every client has at least one subclient.
func (store *leaseStoreImpl) Subclients(id string) int64 {
	return store.leases[id].Subclients
}
