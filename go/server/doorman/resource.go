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

import (
	"path/filepath"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	pb "github.com/youtube/doorman/proto/doorman"
)

// NOTE(ryszard): Any exported Resource methods are responsible for
// taking the lock. Any NOT exported methods, on the other hand, must
// NOT take the lock, even to read. There can be multiple holders of a
// read lock, but any attempt to get the write lock will block further
// read locks. This means that getting a read lock recursively leads
// to deadlocks that are very hard to debug.

// Resource giving clients capacity leases to some resource. It is
// safe to call or access any exported methods or properties from
// multiple goroutines.
type Resource struct {
	// ID is the name the clients use to access this resource.
	ID string // 申请的资源id

	// This mutex guards access to all properties defined below.
	mu sync.RWMutex
	// store contains leases granted to all clients for this
	// resource.
	// 包含授予此资源的所有客户端的租约。
	store LeaseStore

	algorithm           Algorithm
	learner             Algorithm            // 学习模式下的容量分配
	learningModeEndTime time.Time            // 学习模式下的截止时间
	config              *pb.ResourceTemplate //ID对应的资源模板

	// expiryTime is the expiration time for this resource
	// specified by a lower-level server (e.g. by the root server).
	// The root server should ignore it, because it does not have
	// any server which is lower it in the server tree.
	// 是由底层服务器(例如根服务器)指定的该资源的过期时间。
	// 根服务器应该忽略它，因为它在服务器树中没有比它更低的服务器。
	expiryTime *time.Time
}

// capacity returns the current available capacity for res. Note: this
// does not lock the resource, and should be called only when the
// lock is already taken.
// 返回res的当前可用容量。注意:这不会锁定资源，并且应该只在锁已经被占用时调用。
func (res *Resource) capacity() float64 {

	if res.expiryTime != nil && res.expiryTime.Before(time.Now()) {
		// FIXME(rushanny): probably here should be a safe capacity instead.
		return 0.0
	}

	return res.config.GetCapacity()
}

// Release releases any resources held for client. This method is safe
// to call from multiple goroutines.
func (res *Resource) Release(client string) {
	res.mu.Lock()
	defer res.mu.Unlock()
	res.store.Release(client)
}

// SetSafeCapacity sets the safe capacity in a response.
// 设置响应中的安全容量。
func (res *Resource) SetSafeCapacity(resp *pb.ResourceResponse) {
	res.mu.RLock()
	defer res.mu.RUnlock()

	// If the resource configuration does not have a safe capacity
	// configured we return a dynamic safe capacity which equals
	// the capacity divided by the number of clients that we
	// know about.
	// TODO(josv): The calculation of the dynamic safe capacity
	// needs to take sub clients into account (in a multi-server tree).
	// 如果资源配置没有配置安全容量，我们将返回一个动态安全容量，它等于容量除以我们知道的客户端数量。
	// 需要考虑到子客户端(在多服务器树中)。
	if res.config.SafeCapacity == nil {
		resp.SafeCapacity = proto.Float64(*res.config.Capacity / float64(res.store.Count()))
	} else {
		resp.SafeCapacity = proto.Float64(*res.config.SafeCapacity)
	}
}

// Decide runs an algorithm, and returns the leased assigned to
// client. learning should be true if the server is in learning mode.
// 运行一个算法，并返回分配给客户的租赁。如果服务器处于学习模式，则学习应该为真。
func (res *Resource) Decide(request Request) Lease {
	// NOTE(ryszard): Eventually the refresh interval should depend
	// on the level of the server in the tree.
	res.mu.Lock()
	defer res.mu.Unlock()

	// 清理过期租约
	res.store.Clean()

	if res.learningModeEndTime.After(time.Now()) {
		log.V(2).Infof("decision in learning mode for %v", res.ID)
		return res.learner(res.store, res.capacity(), request)
	}
	return res.algorithm(res.store, res.capacity(), request)
}

// LoadConfig loads cfg into the resource. LoadConfig takes care of
// locking the resource.
// config转成resource
func (res *Resource) LoadConfig(cfg *pb.ResourceTemplate, expiryTime *time.Time) {
	res.mu.Lock()
	defer res.mu.Unlock()
	res.config = cfg                   // 模板配置
	res.expiryTime = expiryTime        // 过期时间   新建时设置nil
	algo := cfg.GetAlgorithm()         // 容量分配算法相关配置
	res.algorithm = GetAlgorithm(algo) //容量分配算法
	res.learner = Learn(algo)
}

// Matches returns true if the resource ID matches the glob from cfg.
func (res *Resource) Matches(cfg *pb.ResourceTemplate) bool {
	// NOTE(ryszard): The only possible error from Match is for a
	// malformed pattern, so it is safe to quench it (especially
	// that the config validation should have found any malformed
	// patterns).
	glob := cfg.GetIdentifierGlob()
	matches, _ := filepath.Match(glob, res.ID)
	return glob == res.ID || matches
}

// TODO(ryszard): Make it possible to use a different store.

// newResource returns a new resource named id and configured using
// cfg.
func (server *Server) newResource(id string, cfg *pb.ResourceTemplate) *Resource {
	res := &Resource{
		ID:    id,
		store: NewLeaseStore(id),
	}
	res.LoadConfig(cfg, nil)

	// Calculates the learning mode end time. If one was not specified in the
	// algorithm the learning mode duration equals the lease length, because
	// that is the maximum time after which we can assume clients to have either
	// reported in or lost their lease.
	// 计算学习模式结束时间。
	// 如果在算法中没有指定，则学习模式持续时间等于租约长度，因为这是我们可以假定客户已报告或丢失租约的最大时间。
	algo := res.config.GetAlgorithm()

	// 新创建的resource指定学习模式持续的时间
	var learningModeDuration time.Duration

	if algo.LearningModeDuration != nil {
		learningModeDuration = time.Duration(algo.GetLearningModeDuration()) * time.Second
	} else {
		learningModeDuration = time.Duration(algo.GetLeaseLength()) * time.Second
	}

	res.learningModeEndTime = server.GetLearningModeEndTime(learningModeDuration)

	return res
}

// ResourceStatus is a view of a resource that is useful for
// reporting, eg in /statusz.
type ResourceStatus struct {
	// ID is the id of the resource.
	ID string
	// SumHas is how much capacity has been given to clients.
	SumHas float64
	// SumWants is how much capacity the clients want.
	SumWants float64
	// Capacity is the total available capacity.
	Capacity float64
	// Count is the number of clients.
	Count int64
	// InLeaarningMode is true if the resource is in learning
	// mode.
	InLearningMode bool
	// Algorithm is the algorithm with which this resource is
	// configured.
	Algorithm *pb.Algorithm
}

// Status returns a read-only view of res.
// resource状态信息
func (res *Resource) Status() ResourceStatus {
	res.mu.RLock()
	defer res.mu.RUnlock()

	return ResourceStatus{
		ID:             res.ID,
		SumHas:         res.store.SumHas(),
		SumWants:       res.store.SumWants(),
		Count:          res.store.Count(),
		Capacity:       res.capacity(),
		InLearningMode: res.learningModeEndTime.After(time.Now()),
		Algorithm:      res.config.Algorithm,
	}
}

// ResourceLeaseStatus returns a read-only view of information the outstanding leases for this resource.
func (res *Resource) ResourceLeaseStatus() ResourceLeaseStatus {
	res.mu.RLock()
	defer res.mu.RUnlock()
	return res.store.ResourceLeaseStatus()
}
