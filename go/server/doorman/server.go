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

// Package doorman is a library implementing global, distributed, client-side
// rate limiting.
//
// This is an experimental and incomplete implementation. Most
// notably, a multi-level tree is not supported, and only the most
// basic algorithms are available.
package doorman

import (
	"errors"
	"path/filepath"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/youtube/doorman/go/connection"
	"github.com/youtube/doorman/go/server/election"
	"github.com/youtube/doorman/go/timeutil"
	"golang.org/x/net/context"
	rpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	pb "github.com/youtube/doorman/proto/doorman"
)

var (

	// TODO(rushanny): probably we should get rid of the default vars in the future?

	// defaultPriority is the default priority for the resource request.
	defaultPriority = 1

	// defaultInterval is the default time period after which the server's main loop
	// updates the resources configuration.
	defaultInterval = time.Duration(1 * time.Second)

	// defaultResourceTemplate is the defalt configuration entry for "*" resource.
	// 全局默认的配置
	defaultResourceTemplate = &pb.ResourceTemplate{
		IdentifierGlob: proto.String("*"),
		Capacity:       proto.Float64(0),
		SafeCapacity:   proto.Float64(0),
		Algorithm: &pb.Algorithm{
			Kind:                 pb.Algorithm_FAIR_SHARE.Enum(),
			RefreshInterval:      proto.Int64(int64(defaultInterval.Seconds())),
			LeaseLength:          proto.Int64(20),
			LearningModeDuration: proto.Int64(20),
		},
	}

	// defaultServerCapacityResourceRequest is the default request for "*" resource,
	// which is sent to the lower-level (e.g. root) server only before the server
	// receives actual requests for resources from the clients.
	// 默认的请求参数
	defaultServerCapacityResourceRequest = &pb.ServerCapacityResourceRequest{
		ResourceId: proto.String("*"),
		Wants: []*pb.PriorityBandAggregate{
			{
				Priority:   proto.Int64(int64(defaultPriority)),
				NumClients: proto.Int64(1),
				Wants:      proto.Float64(0.0),
			},
		},
	}
)

const (
	// veryLongTime is used as a max when looking for a minimum
	// duration.
	veryLongTime = 60 * time.Minute

	// minBackoff is the minimum for the exponential backoff.
	minBackoff = 1 * time.Second

	// maxBackoff is the maximum for the exponential backoff.
	maxBackoff = 1 * time.Minute
)

var (
	requestLabels = []string{"method"}

	requests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "doorman",
		Subsystem: "server",
		Name:      "requests",
		Help:      "Requests sent to a Doorman service.",
	}, requestLabels)

	requestErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "doorman",
		Subsystem: "server",
		Name:      "request_errors",
		Help:      "Requests sent to a Doorman service that returned an error.",
	}, requestLabels)

	requestDurations = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "doorman",
		Subsystem: "server",
		Name:      "request_durations",
		Help:      "Duration of different requests in seconds.",
	}, requestLabels)
)

func init() {
	prometheus.MustRegister(requests)
	prometheus.MustRegister(requestErrors)
	prometheus.MustRegister(requestDurations)
}

// Server represents the state of a doorman server.
type Server struct {
	Election election.Election
	ID       string // 当前节点的标识

	// isConfigured is closed once an initial configuration is loaded.
	// 在加载初始配置后关闭。
	isConfigured chan bool

	// mu guards all the properties of server.
	mu             sync.RWMutex
	resources      map[string]*Resource   // 对客户端进行容量分配的结果 从节点为空
	isMaster       bool                   // 当前节点是否是master
	becameMasterAt time.Time              // 变成master节点的时间
	currentMaster  string                 // 当前的master节点ID
	config         *pb.ResourceRepository // 配置的资源模板

	// updater updates the resources' configuration for intermediate server.
	// The root server should ignore it, since it loads the resource
	// configuration from elsewhere.
	updater updater

	// conn contains the configuration of the connection between this
	// server and the lower level server if there is one.
	conn *connection.Connection // 与上层节点的链接

	// quit is used to notify that the server is to be closed.
	// 用于通知服务器将要关闭。
	quit chan bool

	// descs are metrics descriptions for use when the server's state
	// is collected by Prometheus.
	descs struct {
		has        *prometheus.Desc
		wants      *prometheus.Desc
		subclients *prometheus.Desc
	}
}

type updater func(server *Server, retryNumber int) (time.Duration, int)

// WaitUntilConfigured blocks until the server is configured. If the server
// is configured to begin with it immediately returns.
func (server *Server) WaitUntilConfigured() {
	<-server.isConfigured
}

// GetLearningModeEndTime returns the timestamp where a resource that has a
// particular learning mode duration leaves learning mode.
// mode duration is still in learning mode.
// Note: If the learningModeDuration is less or equal to zero there is no
// learning mode!
// 返回具有特定学习模式持续时间的资源离开学习模式的时间戳。
// 模式持续时间仍处于学习模式。注意:如果learningModeDuration小于等于0表示没有学习模式!
// 从变更成主时有用
func (server *Server) GetLearningModeEndTime(learningModeDuration time.Duration) time.Time {
	if learningModeDuration.Seconds() <= 0 {
		return time.Unix(0, 0)
	}

	return server.becameMasterAt.Add(learningModeDuration)
}

// LoadConfig loads config as the new configuration for the server. It
// will take care of any locking, and it will return an error if the
// config is invalid. LoadConfig takes care of locking the server and
// resources. The first call to LoadConfig also triggers taking part
// in the master election, if the relevant locks were specified when
// the server was created.
// 将config加载为服务器的新配置。
// 它将处理任何锁定，如果配置无效，它将返回一个错误。
// LoadConfig负责锁定服务器和资源。
// 如果在创建服务器时指定了相关锁，那么对LoadConfig的第一次调用还会触发参与主选择。
func (server *Server) LoadConfig(ctx context.Context, config *pb.ResourceRepository, expiryTimes map[string]*time.Time) error {
	if err := validateResourceRepository(config); err != nil {
		return err
	}

	server.mu.Lock()
	defer server.mu.Unlock()

	firstTime := server.config == nil

	// Stores the new configuration in the server object.
	// 更新资源配置模板
	server.config = config

	// If this is the first load of a config there are no resources
	// in the server map, so no need to process those, but we do need
	// to let people who were waiting on the server configuration
	// known: for this purpose we close isConfigured channel.
	// Also since we are now a configured server we can
	// start participating in the election process.
	// 如果这是配置的第一次加载，服务器映射中没有资源，所以不需要处理这些资源，
	// 但是我们需要让等待服务器配置的人知道:为此，我们关闭isConfigured通道。
	// 此外，由于我们现在是一个配置的服务器，我们可以开始参与选举过程。
	if firstTime {
		close(server.isConfigured) // 通知配置加载完成
		return server.triggerElection(ctx)
	}

	// Goes through the server's map of resources, loads a new
	// configuration and updates expiration time for each of them.
	// 遍历服务器的资源映射，加载新的配置并更新每个资源的过期时间。
	// 只有从父节点同步回来的才会有过期时间
	for id, resource := range server.resources {
		resource.LoadConfig(server.findConfigForResource(id), expiryTimes[id])
	}

	return nil
}

// performRequests does a request and returns the duration of the
// shortest refresh interval from all handled resources.
//
// If there's an error, it will be logged, and the returned interval
// will be increasing exponentially (basing on the passed retry
// number). The returned nextRetryNumber should be used in the next
// call to performRequests.
// 向父节点进行资源容量申请 子节点的配置模板必须是父节点子集 响应的结果进行更新子接地的配置模板
func (server *Server) performRequests(ctx context.Context, retryNumber int) (time.Duration, int) {
	// Creates new GetServerCapacityRequest.
	in := &pb.GetServerCapacityRequest{ServerId: proto.String(server.ID)}

	server.mu.RLock()

	// Adds all resources in this client's resource registry to the request.
	// 将此客户端的资源注册表中的所有资源添加到请求中。
	for id, resource := range server.resources {
		status := resource.Status()

		// For now we do not take into account clients with different
		// priorities. That is why we form only one PriorityBandAggregate proto.
		// Also, compose request only for the resource whose wants capacity > 0,
		// because it makes no sense to ask for zero capacity.
		// 目前我们没有考虑不同优先级的客户。
		// 这就是为什么我们只形成一个PriorityBandAggregate原型。
		// 另外，只对需要容量 > 0的资源编写请求，因为请求容量为0没有意义。
		if status.SumWants > 0 {
			in.Resource = append(in.Resource, &pb.ServerCapacityResourceRequest{
				ResourceId: proto.String(id),
				// TODO(rushanny): fill optional Has field which is of type Lease.
				Wants: []*pb.PriorityBandAggregate{
					{
						// TODO(rushanny): replace defaultPriority with some client's priority.
						Priority:   proto.Int64(int64(defaultPriority)), // 优先级 没啥用
						NumClients: proto.Int64(status.Count),
						Wants:      proto.Float64(status.SumWants),
					},
				},
			})
		}
	}

	// If there is no actual resources that we could ask for, just send a default request
	// just to check a lower-level server's availability.
	// 如果我们没有实际的资源可以请求，只需发送一个默认的请求来检查一个低级服务器的可用性。
	if len(server.resources) == 0 {
		in.Resource = append(in.Resource, defaultServerCapacityResourceRequest)
	}
	server.mu.RUnlock()

	if retryNumber > 0 {
		log.Infof("GetServerCapacity: retry number %v: %v\n", retryNumber, in)
	}

	out, err := server.getCapacityRPC(ctx, in)
	if err != nil { // 指数退避 返回重试次数
		log.Errorf("GetServerCapacityRequest: %v", err)
		return timeutil.Backoff(minBackoff, maxBackoff, retryNumber), retryNumber + 1
	}

	// Find the minimal refresh interval.
	interval := veryLongTime
	var templates []*pb.ResourceTemplate
	expiryTimes := make(map[string]*time.Time, 0)

	for _, pr := range out.Response {
		_, ok := server.resources[pr.GetResourceId()]
		if !ok {
			log.Errorf("response for non-existing resource: %q", pr.GetResourceId())
			continue
		}

		// Refresh an expiry time for the resource.
		expiryTime := time.Unix(pr.GetGets().GetExpiryTime(), 0)
		expiryTimes[pr.GetResourceId()] = &expiryTime

		// Add a new resource configuration.
		// 新的资源模板配置
		templates = append(templates, &pb.ResourceTemplate{
			IdentifierGlob: proto.String(pr.GetResourceId()),
			Capacity:       proto.Float64(pr.GetGets().GetCapacity()),
			SafeCapacity:   proto.Float64(pr.GetSafeCapacity()),
			Algorithm:      pr.GetAlgorithm(),
		})

		// Find the minimum refresh interval.
		// 最小的刷新间隔
		if refresh := time.Duration(pr.GetGets().GetRefreshInterval()) * time.Second; refresh < interval {
			interval = refresh
		}
	}

	// Append the default template for * resource. It should be the last one in templates.
	// 添加 * resource的默认模板。它应该是模板中的最后一个。
	templates = append(templates, proto.Clone(defaultResourceTemplate).(*pb.ResourceTemplate))

	// Load a new configuration for the resources.
	// 重新更新配置模板
	if err := server.LoadConfig(ctx, &pb.ResourceRepository{
		Resources: templates,
	}, expiryTimes); err != nil {
		log.Errorf("server.LoadConfig: %v", err)
		return timeutil.Backoff(minBackoff, maxBackoff, retryNumber), retryNumber + 1
	}

	// Applies the --minimum_refresh_interval_secs flag.
	// Or if interval was set to veryLongTime and not updated, set it to minimum refresh interval.
	// 更新刷新间隔
	if interval < server.conn.Opts.MinimumRefreshInterval || interval == veryLongTime {
		log.Infof("overriding interval %v with %v", interval, server.conn.Opts.MinimumRefreshInterval)
		interval = server.conn.Opts.MinimumRefreshInterval
	}

	return interval, 0
}

// getCapacityRPC Executes this RPC against the current master. Returns the GetServerCapacity RPC
// response, or nil if an error occurred.
// 对当前主服务器执行此RPC。返回GetServerCapacity RPC响应，如果发生错误则返回nil。
func (server *Server) getCapacityRPC(ctx context.Context, in *pb.GetServerCapacityRequest) (*pb.GetServerCapacityResponse, error) {
	out, err := server.conn.ExecuteRPC(func() (connection.HasMastership, error) {
		return server.conn.Stub.GetServerCapacity(ctx, in)

	})

	// Returns an error if we could not execute the RPC.
	if err != nil {
		return nil, err
	}

	// Returns the result from the RPC to the caller.
	return out.(*pb.GetServerCapacityResponse), err
}

// IsMaster returns true if server is the master.
func (server *Server) IsMaster() bool {
	server.mu.RLock()
	defer server.mu.RUnlock()
	return server.isMaster
}

// CurrentMaster returns the current master, or an empty string if
// there's no master or it is unknown.
// 返回当前主节点，如果没有主节点或未知则返回空字符串。
func (server *Server) CurrentMaster() string {
	server.mu.RLock()
	defer server.mu.RUnlock()
	return server.currentMaster
}

func validateGetCapacityRequest(p *pb.GetCapacityRequest) error {
	if p.GetClientId() == "" {
		return errors.New("client_id cannot be empty")
	}
	seen := make(map[string]bool)
	for _, r := range p.Resource {
		if err := validateResourceRequest(r); err != nil {
			return err
		}
		seen[r.GetResourceId()] = true
	}

	return nil
}

func validateResourceRequest(p *pb.ResourceRequest) error {
	if p.GetResourceId() == "" {
		return errors.New("resource_id cannot be empty")
	}
	if p.GetWants() < 0 {
		return errors.New("capacity must be positive")
	}
	return nil
}

// validateResourceRepository returns an error if p is not correct. It
// must contain an entry for "*" which must also be the last entry.
func validateResourceRepository(p *pb.ResourceRepository) error {
	starFound := false
	for i, res := range p.Resources {
		glob := res.GetIdentifierGlob()
		// All globs have to be well formed.
		//
		// NOTE(ryszard): filepath.Match will NOT return an
		// error if the glob is matched against the empty
		// string.
		if _, err := filepath.Match(glob, " "); err != nil {
			return err
		}

		// If there is an algorithm in this entry, validate it.
		if algo := res.Algorithm; algo != nil {
			if algo.RefreshInterval == nil || algo.LeaseLength == nil {
				return errors.New("must have a refresh interval and a lease length")
			}

			if *algo.RefreshInterval < 1 {
				return errors.New("invalid refresh interval, must be at least 1 second")
			}

			if *algo.LeaseLength < 1 {
				return errors.New("Invalid lease length, must be at least 1 second")
			}

			if *algo.LeaseLength < *algo.RefreshInterval {
				return errors.New("Lease length must be larger than the refresh interval")
			}
		}

		// * has to contain an algorithm and be the last
		// entry.
		if glob == "*" {
			if res.Algorithm == nil {
				return errors.New("the entry for * must specify an algorithm")
			}
			if i+1 != len(p.Resources) {
				return errors.New(`the entry for "*" must be the last one`)
			}
			starFound = true
		}
	}

	if !starFound {
		return errors.New(`the resource respository must contain at least an entry for "*"`)
	}

	return nil
}

// handleElectionOutcome observes the results of master elections and
// updates the server to reflect acquired or lost mastership.
// 观察master选举的结果，并更新服务器以反映获得或失去的master地位。
func (server *Server) handleElectionOutcome() {
	for isMaster := range server.Election.IsMaster() {
		server.mu.Lock()
		server.isMaster = isMaster

		if isMaster {
			log.Info("this Doorman server is now the master")
			server.becameMasterAt = time.Now()
			server.resources = make(map[string]*Resource)
		} else {
			log.Warning("this Doorman server lost mastership")
			server.becameMasterAt = time.Unix(0, 0)
			server.resources = nil // 从节点不进行容量分配
		}

		server.mu.Unlock()
	}
}

// handleMasterID observes the IDs of elected masters and makes them
// available through CurrentMaster.
func (server *Server) handleMasterID() {
	for newMaster := range server.Election.Current() {
		server.mu.Lock()

		if newMaster != server.currentMaster {
			log.Infof("setting current master to '%v'", newMaster)
			server.currentMaster = newMaster
		}

		server.mu.Unlock()
	}
}

// triggerElection makes the server run in a Chubby Master2 election.
// 选主
func (server *Server) triggerElection(ctx context.Context) error {
	if err := server.Election.Run(ctx, server.ID); err != nil {
		return err
	}
	go server.handleElectionOutcome()
	go server.handleMasterID()

	return nil
}

// New returns a new unconfigured server. parentAddr is the address of
// a parent, pass the empty string to create a root server. This
// function should be called only once, as it registers metrics.
// 返回一个新的未配置的服务器。
// parentAddr是父服务器的地址，传递空字符串来创建根服务器。
// 这个函数应该只调用一次，因为它注册了指标。
func New(ctx context.Context, id string, parentAddr string, leader election.Election, opts ...connection.Option) (*Server, error) {
	s, err := NewIntermediate(ctx, id, parentAddr, leader, opts...)
	if err != nil {
		return nil, err
	}

	return s, prometheus.Register(s)
}

// Describe implements prometheus.Collector.
func (server *Server) Describe(ch chan<- *prometheus.Desc) {
	ch <- server.descs.has
	ch <- server.descs.wants
	ch <- server.descs.subclients
}

// Collect implements prometheus.Collector.
func (server *Server) Collect(ch chan<- prometheus.Metric) {
	status := server.Status()

	for id, res := range status.Resources {
		ch <- prometheus.MustNewConstMetric(server.descs.has, prometheus.GaugeValue, res.SumHas, id)
		ch <- prometheus.MustNewConstMetric(server.descs.wants, prometheus.GaugeValue, res.SumWants, id)
		ch <- prometheus.MustNewConstMetric(server.descs.subclients, prometheus.GaugeValue, float64(res.Count), id)
	}
}

// NewIntermediate creates a server connected to the lower level server.
// addr 父节点地址
// 向父节点申请容量 用于向客户端进行容量分配
func NewIntermediate(ctx context.Context, id string, addr string, leader election.Election, opts ...connection.Option) (*Server, error) {
	var (
		conn    *connection.Connection
		updater updater
		err     error
	)

	isRootServer := addr == ""

	// Set up some configuration for intermediate server: establish a connection
	// to a lower-level server (e.g. the root server) and assign the updater function.
	// 非根节点 建立上层节点的链接
	if !isRootServer {
		if conn, err = connection.New(addr, opts...); err != nil {
			return nil, err
		}

		// 向上级节点进行容量申请
		updater = func(server *Server, retryNumber int) (time.Duration, int) {
			return server.performRequests(ctx, retryNumber)
		}
	}

	server := &Server{
		ID:             id,
		Election:       leader,
		isConfigured:   make(chan bool),
		resources:      make(map[string]*Resource),
		becameMasterAt: time.Unix(0, 0),
		conn:           conn,
		updater:        updater,
		quit:           make(chan bool),
	}

	const (
		namespace = "doorman"
		subsystem = "server"
	)

	labelNames := []string{"resource"}
	server.descs.has = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subsystem, "has"),
		"All capacity assigned to clients for a resource.",
		labelNames, nil,
	)
	server.descs.wants = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subsystem, "wants"),
		"All capacity requested by clients for a resource.",
		labelNames, nil,
	)
	server.descs.subclients = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subsystem, "subclients"),
		"Number of clients requesting this resource.",
		labelNames, nil,
	)

	// For an intermediate server load the default config for "*"
	// resource.  As for root server, this config will be loaded
	// from some external source..
	// 非根节点 加载默认配置
	if !isRootServer {
		if err := server.LoadConfig(ctx, &pb.ResourceRepository{
			Resources: []*pb.ResourceTemplate{
				proto.Clone(defaultResourceTemplate).(*pb.ResourceTemplate),
			},
		}, map[string]*time.Time{}); err != nil {
			return nil, err
		}
	}

	go server.run()

	return server, nil
}

// run is the server's main loop. It takes care of requesting new resources,
// and managing ones already claimed. This is the only method that should be
// performing RPC.
// Run是服务器的主循环。
// 它负责向父节点请求新资源，并管理客户端已请求的资源。这是唯一应该执行RPC的方法。
func (server *Server) run() {
	interval := defaultInterval
	retryNumber := 0

	for {
		var wakeUp <-chan time.Time
		if server.updater != nil {
			wakeUp = time.After(interval)
		}

		select {
		case <-server.quit:
			// The server is closed, nothing to do here.
			return
		case <-wakeUp:
			// Time to update the resources configuration.
			interval, retryNumber = server.updater(server, retryNumber)
		}
	}
}

// Close closes the doorman server.
// 关闭doorman服务。
func (server *Server) Close() {
	server.quit <- true
}

// findConfigForResource find the configuration template that applies
// to a specific resource. This function panics if if cannot find a
// suitable template, which should never happen because there is always
// a configuration entry for "*".
// 找到应用于特定资源的配置模板。如果找不到合适的模板，这个函数就会恐慌，这应该永远不会发生，因为总是有一个“*”的配置项。
func (server *Server) findConfigForResource(id string) *pb.ResourceTemplate {
	// Try to match it literally. 精确匹配模板
	for _, tpl := range server.config.Resources {
		if tpl.GetIdentifierGlob() == id {
			return tpl
		}
	}

	// See if there's a template that matches as a pattern. 模糊匹配模板
	for _, tpl := range server.config.Resources {
		glob := tpl.GetIdentifierGlob()
		matched, err := filepath.Match(glob, id)

		if err != nil {
			log.Errorf("Error trying to match %v to %v", id, glob)
			continue
		} else if matched {
			return tpl
		}
	}

	// This should never happen
	panic(id)
}

// getResource takes a resource identifier and returns the matching
// resource (which will be created if necessary).
// id 资源模板Id
func (server *Server) getOrCreateResource(id string) *Resource {
	server.mu.Lock()
	defer server.mu.Unlock()

	// Resource already exists in the server state; return it.
	// Resource已存在 直接返回
	if res, ok := server.resources[id]; ok {
		return res
	}

	// 必须找出来模板 否则panic
	resource := server.newResource(id, server.findConfigForResource(id))
	server.resources[id] = resource

	return resource
}

// ReleaseCapacity releases capacity owned by a client.
// 释放客户端拥有的容量。
func (server *Server) ReleaseCapacity(ctx context.Context, in *pb.ReleaseCapacityRequest) (out *pb.ReleaseCapacityResponse, err error) {
	out = new(pb.ReleaseCapacityResponse)

	log.V(2).Infof("ReleaseCapacity req: %v", in)
	start := time.Now()
	requests.WithLabelValues("ReleaseCapacity").Inc()
	defer func() {
		log.V(2).Infof("ReleaseCapacity res: %v", out)
		requestDurations.WithLabelValues("ReleaseCapacity").Observe(time.Since(start).Seconds())
		if err != nil {
			requestErrors.WithLabelValues("ReleaseCapacity").Inc()

		}
	}()

	// If we are not the master we tell the client who we think the master
	// is and we return. There are some subtleties around this: The presence
	// of the mastership field signifies that we are not the master. The
	// presence of the master_bns field inside mastership signifies whether
	// we know who the master is or not.
	if !server.IsMaster() {
		out.Mastership = &pb.Mastership{}

		if server.currentMaster != "" {
			out.Mastership.MasterAddress = proto.String(server.currentMaster)
		}

		return out, nil
	}

	client := in.GetClientId()

	// Takes the server lock because we are reading the resource map below.
	// 获取服务器锁，因为我们正在读取下面的资源映射。
	server.mu.RLock()
	defer server.mu.RUnlock()

	for _, resourceID := range in.ResourceId {
		// If the server does not know about the resource we don't have to do
		// anything.
		// 如果服务器不知道资源，我们不需要做任何事情。
		if res, ok := server.resources[resourceID]; ok {
			res.store.Release(client)
		}
	}

	return out, nil
}

// item is the mapping between the client id and the lease that algorithm assigned to the client with this id.
type item struct {
	id    string
	lease Lease
}

type clientRequest struct {
	client     string
	resID      string
	has        float64
	wants      float64
	subclients int64
}

// GetCapacity assigns capacity leases to clients. It is part of the
// doorman.CapacityServer implementation.
// 向客户分配容量租约。
func (server *Server) GetCapacity(ctx context.Context, in *pb.GetCapacityRequest) (out *pb.GetCapacityResponse, err error) {
	out = new(pb.GetCapacityResponse)

	log.V(2).Infof("GetCapacity req: %v", in)

	start := time.Now()
	requests.WithLabelValues("GetCapacity").Inc()
	defer func() {
		log.V(2).Infof("GetCapacity res: %v", out)
		requestDurations.WithLabelValues("GetCapacity").Observe(time.Since(start).Seconds())
		if err != nil {
			requestErrors.WithLabelValues("GetCapacity").Inc()
		}
	}()

	// If we are not the master, we redirect the client.
	if !server.IsMaster() {
		master := server.CurrentMaster()
		out.Mastership = &pb.Mastership{}

		if master != "" {
			out.Mastership.MasterAddress = proto.String(master)
		}
		return out, nil
	}

	client := in.GetClientId()

	// We will create a new goroutine for every resource in the
	// request. This is the channel that the leases come back on.
	itemsC := make(chan item, len(in.Resource))

	// requests will keep information about all resource requests that
	// the specified client sent at the moment.
	var requests []clientRequest

	for _, req := range in.Resource {
		request := clientRequest{
			client:     client,
			resID:      req.GetResourceId(),
			has:        req.GetHas().GetCapacity(),
			wants:      req.GetWants(),
			subclients: 1,
		}

		requests = append(requests, request)
	}

	server.getCapacity(requests, itemsC)

	// We collect the assigned leases.
	for range in.Resource {
		item := <-itemsC
		resp := &pb.ResourceResponse{
			ResourceId: proto.String(item.id),
			Gets: &pb.Lease{
				RefreshInterval: proto.Int64(int64(item.lease.RefreshInterval.Seconds())),
				ExpiryTime:      proto.Int64(item.lease.Expiry.Unix()),
				Capacity:        proto.Float64(item.lease.Has),
			},
		}
		server.getOrCreateResource(item.id).SetSafeCapacity(resp)
		out.Response = append(out.Response, resp)
	}

	return out, nil
}

// 获取容量申请请求的结果
func (server *Server) getCapacity(crequests []clientRequest, itemsC chan item) {
	for _, creq := range crequests {
		res := server.getOrCreateResource(creq.resID)
		req := Request{
			Client:     creq.client,
			Has:        creq.has,
			Wants:      creq.wants,
			Subclients: creq.subclients,
		}

		go func(req Request) {
			itemsC <- item{
				id:    res.ID,
				lease: res.Decide(req),
			}
		}(req)
	}
}

// GetServerCapacity gives capacity to doorman servers that can assign
// to their clients. It is part of the doorman.CapacityServer
// implementation.
// 当前doorman节点向上级节点进行容量申请
func (server *Server) GetServerCapacity(ctx context.Context, in *pb.GetServerCapacityRequest) (out *pb.GetServerCapacityResponse, err error) {
	out = new(pb.GetServerCapacityResponse)
	// TODO: add metrics for getServerCapacity latency and requests count.
	log.V(2).Infof("GetServerCapacity req: %v", in)
	defer log.V(2).Infof("GetServerCapacity res: %v", out)

	// If we are not the master, we redirect the client.
	if !server.IsMaster() {
		master := server.CurrentMaster()
		out.Mastership = &pb.Mastership{}

		if master != "" {
			out.Mastership.MasterAddress = proto.String(master)
		}

		return out, nil
	}

	client := in.GetServerId()

	// We will create a new goroutine for every resource in the
	// request. This is the channel that the leases come back on.
	itemsC := make(chan item, len(in.Resource))

	// requests will keep information about all resource requests that
	// the specified client sent at the moment.
	var requests []clientRequest

	for _, req := range in.Resource {
		var (
			wantsTotal      float64 // 总的请求容量
			subclientsTotal int64   // 总的客户端数
		)

		// Calaculate total number of subclients and overall wants
		// capacity that they ask for.
		// 计算子客户端总数和他们要求的总容量。只会有一个
		for _, wants := range req.Wants {
			wantsTotal += wants.GetWants()

			// Validate number of subclients which should be not less than 1,
			// because every server has at least one subclient: itself.
			// 验证的子客户端数量应该不小于1，因为每个服务器至少有一个子客户端:它自己。
			subclients := wants.GetNumClients()
			if subclients < 1 {
				return nil, rpc.Errorf(codes.InvalidArgument, "subclients should be > 0")
			}
			subclientsTotal += wants.GetNumClients()
		}

		request := clientRequest{
			client:     client,
			resID:      req.GetResourceId(),
			has:        req.GetHas().GetCapacity(),
			wants:      wantsTotal,
			subclients: subclientsTotal,
		}

		requests = append(requests, request)
	}

	server.getCapacity(requests, itemsC)

	// We collect the assigned leases.
	for range in.Resource {
		item := <-itemsC
		resp := &pb.ServerCapacityResourceResponse{
			ResourceId: proto.String(item.id),
			Gets: &pb.Lease{
				RefreshInterval: proto.Int64(int64(item.lease.RefreshInterval.Seconds())),
				ExpiryTime:      proto.Int64(item.lease.Expiry.Unix()),
				Capacity:        proto.Float64(item.lease.Has), // 给子级节点分配的容量
			},
			Algorithm:    server.resources[item.id].config.GetAlgorithm(),
			SafeCapacity: proto.Float64(server.resources[item.id].config.GetSafeCapacity()),
		}

		out.Response = append(out.Response, resp)
	}

	return out, nil
}

// Discovery implements the Discovery RPC which can be used to discover the address of the master.
// 实现Discovery RPC，该RPC可用于发现主服务器的地址。
func (server *Server) Discovery(ctx context.Context, in *pb.DiscoveryRequest) (out *pb.DiscoveryResponse, err error) {
	out = new(pb.DiscoveryResponse)

	out.IsMaster = proto.Bool(server.isMaster)
	out.Mastership = &pb.Mastership{}
	master := server.CurrentMaster()

	if master != "" {
		out.Mastership.MasterAddress = proto.String(master)
	}

	return out, nil
}

// ServerStatus is a read-only view of a server suitable for
// reporting, eg in /statusz.
type ServerStatus struct {
	// IsMaster is true if the server is a master.
	IsMaster bool
	// Election contains information related to the master  election.
	Election election.Election
	// CurrentMaster is the id of the current master.
	CurrentMaster string
	// Resources are the statuses of the resources managed by this
	// server.
	Resources map[string]ResourceStatus
	// Config is the human readable representation of this server's
	// config.
	Config string
}

// Status returns a read-only view of server.
func (server *Server) Status() ServerStatus {
	server.mu.RLock()
	defer server.mu.RUnlock()
	resources := make(map[string]ResourceStatus, len(server.resources))

	for k, v := range server.resources {
		resources[k] = v.Status()
	}
	return ServerStatus{
		IsMaster:      server.isMaster,
		Election:      server.Election,
		CurrentMaster: server.currentMaster,
		Resources:     resources,
		Config:        proto.MarshalTextString(server.config),
	}
}

// ResourceLeaseStatus returns a read-only view of of the leases on a resource owned by this server.
func (server *Server) ResourceLeaseStatus(id string) ResourceLeaseStatus {
	server.mu.RLock()
	defer server.mu.RUnlock()
	res, ok := server.resources[id]
	if !ok {
		log.Errorf("ResourceLeaseStatus: no resource with ID %v. Returning empty status.", id)
		return ResourceLeaseStatus{}
	}
	return res.ResourceLeaseStatus()
}
