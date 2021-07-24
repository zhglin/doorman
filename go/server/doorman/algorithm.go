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
	"time"

	log "github.com/golang/glog"

	pb "github.com/youtube/doorman/proto/doorman"
)

// Request specifies a requested capacity lease that an Algorithm may
// grant.
type Request struct {
	// Client is the identifier of a client requesting capacity.
	Client string

	// Has is the capacity the client claims it has been assigned
	// previously.
	Has float64

	// Wants is the capacity that the client desires.
	Wants float64

	// Subclients is the number of subclients that the client has.
	Subclients int64
}

// Algorithm is a function that takes a LeaseStore, the total
// available capacity, and a Request, and returns the lease.
type Algorithm func(store LeaseStore, capacity float64, request Request) Lease

func getAlgorithmParams(config *pb.Algorithm) (leaseLength, refreshInterval time.Duration) {
	return time.Duration(config.GetLeaseLength()) * time.Second, time.Duration(config.GetRefreshInterval()) * time.Second
}

func minF(left, right float64) float64 {
	if left > right {
		return right
	}
	return left
}

func maxF(left, right float64) float64 {
	if left > right {
		return left
	}
	return right
}

// NoAlgorithm returns the zero algorithm: every clients gets as much
// capacity as it asks for.
// 这种伪算法总是返回所需的容量，使其本质上是一个空操作。
// 对于这个算法，配置的容量是没有意义的。
func NoAlgorithm(config *pb.Algorithm) Algorithm {
	length, interval := getAlgorithmParams(config)

	return func(store LeaseStore, capacity float64, r Request) Lease {
		return store.Assign(r.Client, length, interval, r.Wants, r.Wants, r.Subclients)
	}
}

// Static assigns to each client the same configured capacity. Note
// that this algorithm attaches a different meaning to config.Capacity
// - it is the per client assigned capacity, not the total capacity
// available to all clients.
// 此算法返回所需（所需）容量和静态容量（在配置中指定）中的最小值。这意味着如果客户端要求的容量少于配置的容量，它将得到它所要求的容量。如果客户端要求的容量超过配置的容量，它将获得该容量。
// 对于此算法，配置的资源容量不是资源的最大容量，而是每个请求返回的最大容量。
func Static(config *pb.Algorithm) Algorithm {
	length, interval := getAlgorithmParams(config)

	return func(store LeaseStore, capacity float64, r Request) Lease {
		return store.Assign(r.Client, length, interval, minF(capacity, r.Wants), r.Wants, r.Subclients)
	}
}

// FairShare assigns to each client a "fair" share of the available
// capacity. The definition of "fair" is as follows: In case the total
// wants is below the total available capacity, everyone gets what
// they want.  If however the total wants is higher than the total
// available capacity everyone is guaranteed their equal share of the
// capacity. Any capacity left by clients who ask for less than their
// equal share is distributed to clients who ask for more than their
// equal share in a way so that available capacity is partitioned in
// equal parts in an iterative way.
// 分配给每个客户一个“公平的”份额的可用容量。
// “公平”的定义如下:如果总需求低于总可用容量，则每个人都能得到他们想要的。
// 然而，如果总需求高于总可用容量，则保证每个人都能平等分享容量。
// 如果客户要求的容量小于其平均份额，则将其剩余的容量分配给要求的容量大于其平均份额的客户，
// 这样可用容量就会以迭代的方式被平均分配。
func FairShare(config *pb.Algorithm) Algorithm {
	length, interval := getAlgorithmParams(config)

	return func(store LeaseStore, capacity float64, r Request) Lease {
		// Get the lease for this client in the store. If it's not
		// there, that's fine, we will get the zero lease (which works
		// for the calculations that come next).
		// 获取租约。如果它不存在，那也没关系，我们将得到零租约(这对接下来的计算有效)
		old := store.Get(r.Client)

		// A sanity check: what the client thinks it has should match
		// what the server thinks it has. This is not a problem for
		// now, as the server will depend on its own version, but it
		// may be a sign of problems with a client implementation, so it's better to log it.
		// 完整性检查:客户端认为它拥有的应该与服务器认为它拥有的相匹配。
		// 目前这还不是问题，因为服务器将依赖于它自己的版本，但这可能是客户机实现存在问题的迹象，所以最好将其记录下来。
		if r.Has != old.Has {
			log.Errorf("client %v is confused: says it has %v, was assigned %v.", r.Client, r.Has, old.Has)
		}

		// Find the number of subclients that we know of. Note that
		// the number of sublclients for this request's client may
		// have changed, so we have to take it in account.
		// 找出我们所知道的子客户机的数量。注意，这个请求的客户端的子客户端数量可能已经改变，所以我们必须考虑它。
		// 当前总的子客服机数量  当前总的-当前client之前的 + 当前client最新的
		count := store.Count() - old.Subclients + r.Subclients

		// Let's find see how much capacity is actually available. We
		// can count into that the capacity the client has been
		// assigned.
		// 让我们看看实际有多少容量可用。我们可以计算客户端已分配的容量。
		// 剩余的容量 配置的总容量-已分配的总容量(包含当前client已分配的) + 当前client已分配的
		available := capacity - store.SumHas() + old.Has

		// Amount of capacity that each subclient is entitled to.
		// 每个子客户端有权获得的容量。用总容量进行计算
		equalShare := capacity / float64(count)

		// How much capacity this client's subclients are entitled to.
		// 该客户的子客户有权获得多少容量。
		deservedShare := equalShare * float64(r.Subclients)

		// If the client is asking for less than it deserves, that's
		// fine with us. We can just give it to it, assuming that
		// there's enough capacity available.
		// 如果客户要求的比应得的少，我们也没有意见。我们可以把它给它，假设有足够的可用容量。
		if r.Wants <= deservedShare {
			return store.Assign(r.Client, length, interval, minF(r.Wants, available), r.Wants, r.Subclients)
		}

		// If we reached here, it means that the client wants more
		// than its fair share. We have to find out the extent to
		// which we can accomodate it.
		// 如果我们达到这里，那就意味着客户想要比公平份额更多的份额。我们必须找出我们能在多大程度上适应它。
		var (
			// First round of capacity redistribution. extra is the
			// capacity left by clients asking for less than their
			// fair share.
			// 第一轮产能再分配。额外的是客户要求低于公平份额的剩余容量。
			extra float64

			// wantExtra is the number of subclients that are competing
			// for the extra capacity. We know that the current client is
			// there.
			// wantExtra是竞争额外容量的子客户端数量。我们知道当前的客户在那里。
			wantExtra = r.Subclients

			// Save all the clients that want extra capacity. We'll
			// need this list for the second round of extra
			// distribution.
			// 保存所有需要额外容量的客户端。我们需要这份名单进行第二轮额外分配。
			wantExtraClients = make(map[string]Lease)
		)

		store.Map(func(id string, lease Lease) {
			if id == r.Client {
				return
			}
			// 应该平均分类的量
			deserved := float64(lease.Subclients) * equalShare
			if lease.Wants < deserved {
				// Wants less than it deserves. Put the unclaimed
				// capacity in the extra pool
				// 想要的比应得的少。将无人认领的容量放入额外的池中
				extra += deserved - lease.Wants
			} else if lease.Wants > deserved {
				// Wants more than it deserves. Add its clients to the
				// extra contenders.
				// 想要的比应得的多。把它的客户加入到额外的竞争者中。
				wantExtra += lease.Subclients
				wantExtraClients[id] = lease
			}
		})

		// deservedExtra is the chunk of the extra pool this client is
		// entitled to.
		// deservedExtra是这个客户端有权使用的额外池的块。
		deservedExtra := (extra / float64(wantExtra)) * float64(r.Subclients)

		// The client wants some extra, but less than to what it is
		// entitled.
		// 客户想要一些额外的，但比它应得的少。
		if r.Wants < deservedShare+deservedExtra {
			return store.Assign(r.Client, length, interval, minF(r.Wants, available), r.Wants, r.Subclients)
		}

		// Second round of extra capacity distribution: some clients
		// may have asked for more than their fair share, but less
		// than the chunk of extra that they are entitled to. Let's
		// redistribute that.
		// 第二轮额外容量分配:一些客户要求的可能超过了他们应得的份额，但却低于他们第一轮应得的份额。让我们重新分配。
		var (
			wantExtraExtra = r.Subclients
			extraExtra     float64
		)
		for id, lease := range wantExtraClients {
			if id == r.Client {
				continue
			}

			if lease.Wants < deservedExtra+deservedShare {
				extraExtra += deservedExtra + deservedShare - lease.Wants // 多余的
			} else if lease.Wants > deservedExtra+deservedShare {
				wantExtraExtra += lease.Subclients // 超出的客户端数
			}
		}
		deservedExtraExtra := (extraExtra / float64(wantExtraExtra)) * float64(r.Subclients)
		return store.Assign(r.Client, length, interval, minF(deservedShare+deservedExtra+deservedExtraExtra, available), r.Wants, r.Subclients)
	}
}

// ProportionalShare assigns to each client what it wants, except
// in an overload situation where each client gets at least an equal
// share (if it wants it), plus (if possible) a share of the capacity
// left on the table by clients who are requesting less than their
// equal share proportional to what the client is asking for.
// 比例共享分配 超出的按比例分配
func ProportionalShare(config *pb.Algorithm) Algorithm {
	length, interval := getAlgorithmParams(config)
	return func(store LeaseStore, capacity float64, r Request) Lease {
		var (
			count = store.Count()
			old   = store.Get(r.Client)
			gets  = 0.0
		)

		// If this is a new client, we adjust the count.
		// 新加的客户端 增加subclients
		if !store.HasClient(r.Client) {
			count += r.Subclients
		}

		// This is the equal share that every subclient has an absolute
		// claim on.
		// 这是每个子客户都拥有绝对所有权的平等份额。
		equalShare := capacity / float64(count)

		// This is the equal share that should be assigned to the current client
		// based on the number of its subclients.
		// 这是根据当前客户端的子客户端数量分配给当前客户端的平均份额。
		equalSharePerClient := equalShare * float64(r.Subclients)

		// This is the capacity which is currently unused (assuming that
		// the requesting client has no capacity). It is the maximum
		// capacity that this run of the algorithm can assign to the
		// requesting client.
		// 这是当前未使用的容量(假设请求客户端没有容量)。它是此运行算法可以分配给请求客户机的最大容量。
		unusedCapacity := capacity - store.SumHas() + old.Has

		// If the client wants less than it equal share or
		// if the sum of what all clients want together is less
		// than the available capacity we can give this client what
		// it wants.
		// 如果客户端想要小于它的平均份额，或者如果所有客户端想要的总和小于可用容量，我们可以给这个客户端它想要的。
		if store.SumWants() <= capacity || r.Wants <= equalSharePerClient {
			return store.Assign(r.Client, length, interval,
				minF(r.Wants, unusedCapacity), r.Wants, r.Subclients)
		}

		// We now need to determine if we can give a top-up on
		// the equal share. The capacity for this top up comes
		// from clients who want less than their equal share,
		// so we calculate how much capacity this is. We also
		// calculate the excess need of all the clients, which
		// is the sum of all the wants over the equal share.
		// 我们现在需要确定我们是否能在平均份额的基础上再加一份。
		// 这个充值的容量来自那些想要比他们的平均份额少的客户，所以我们计算这个容量是多少。
		// 我们还计算所有客户的超额需求，即所有需求超过平均份额的总和。
		extraCapacity := 0.0
		extraNeed := 0.0

		store.Map(func(id string, lease Lease) {
			var wants float64
			var subclients int64

			if id == r.Client {
				wants = r.Wants
				subclients = r.Subclients
			} else {
				wants = lease.Wants
				subclients = lease.Subclients
			}

			// Every client should receive the resource capacity based on the number
			// of subclients it has.
			// 每个客户端应该根据它所拥有的子客户端数量接收资源容量。
			equalSharePerClient := equalShare * float64(subclients)
			if wants < equalSharePerClient {
				extraCapacity += equalSharePerClient - wants // 超出的
			} else {
				extraNeed += wants - equalSharePerClient // 不足的
			}
		})

		// Every client with a need over the equal share will get
		// a proportional top-up.
		// 每个需要超过平均份额的客户都将获得按比例的充值。 按比例平均分配
		gets = equalSharePerClient + (r.Wants-equalSharePerClient)*(extraCapacity/extraNeed)

		// Insert the capacity grant into the lease store. We cannot
		// give out more than the currently unused capacity. If that
		// is less than what the algorithm calculated we will
		// adjust this in the next capacity refreshes.
		// 将容量授予插入到租赁存储中。我们不能提供超过目前未使用的容量。
		// 如果这比算法计算的要少，在下一次容量刷新中调整此参数。
		return store.Assign(r.Client, length, interval,
			minF(gets, unusedCapacity), r.Wants, r.Subclients)
	}
}

// Learn returns the algorithm used in learning mode. It assigns to
// the client the same capacity it reports it had before.
// 返回在学习模式中使用的算法。它分配给客户端与之前报告的容量相同的容量。
func Learn(config *pb.Algorithm) Algorithm {
	length, interval := getAlgorithmParams(config)
	return func(store LeaseStore, capacity float64, r Request) Lease {
		return store.Assign(r.Client, length, interval, r.Has, r.Wants, r.Subclients)
	}
}

// 不同的流量分配类型对应的算法规则
var algorithms = map[pb.Algorithm_Kind]func(*pb.Algorithm) Algorithm{
	pb.Algorithm_NO_ALGORITHM:       NoAlgorithm,
	pb.Algorithm_STATIC:             Static,
	pb.Algorithm_PROPORTIONAL_SHARE: ProportionalShare,
	pb.Algorithm_FAIR_SHARE:         FairShare,
}

func GetAlgorithm(config *pb.Algorithm) Algorithm {
	return algorithms[config.GetKind()](config)
}
