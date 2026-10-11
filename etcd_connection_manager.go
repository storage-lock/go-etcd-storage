package etcd_storage

import (
	"context"
	"github.com/storage-lock/go-storage"
	clientv3 "go.etcd.io/etcd/client/v3"
	"sync"
)

// etcdNewClient 默认创建真实客户端，测试可替换。
var etcdNewClient = clientv3.New

// EtcdConnectionManager 负责维护与 etcd 的连接
type EtcdConnectionManager struct {

	// etcd 端点列表
	Endpoints []string

	// initMu 保护下面的初始化状态。原用 sync.Once，Connect 首次失败后 err 永久缓存，
	// 后续 Take 永远返回同一 err 无法重试（漏洞4/liveness）。改为 mutex + initialized 标志：
	// 成功后缓存 client 不再重复 Connect；失败不缓存 err，下次 Take 可重试。
	initMu      sync.Mutex
	initialized bool
	client      *clientv3.Client
}

var _ storage.ConnectionManager[*clientv3.Client] = &EtcdConnectionManager{}

// NewEtcdConnectionManagerFromEndpoints 从 etcd 端点列表创建连接管理器
func NewEtcdConnectionManagerFromEndpoints(endpoints []string) *EtcdConnectionManager {
	return &EtcdConnectionManager{
		Endpoints: endpoints,
	}
}

// NewEtcdConnectionManagerFromClient 复用已经存在的 etcd 客户端，从其创建连接管理器
func NewEtcdConnectionManagerFromClient(client *clientv3.Client) *EtcdConnectionManager {
	return &EtcdConnectionManager{
		client:      client,
		initialized: true, // 外部预设 client，Take 不再 Connect
	}
}

const EtcdConnectionManagerName = "etcd-connection-manager"

func (x *EtcdConnectionManager) Name() string {
	return EtcdConnectionManagerName
}

// Take 获取 etcd 客户端。
//
// 漏洞4 修复：原 sync.Once 在连接失败时永久缓存 err，无法重试。
// 改为 mutex + initialized：成功后缓存 client 不重复 Connect；失败不缓存 err 可重试。
func (x *EtcdConnectionManager) Take(ctx context.Context) (*clientv3.Client, error) {
	x.initMu.Lock()
	defer x.initMu.Unlock()
	if x.initialized {
		return x.client, nil
	}
	client, err := etcdNewClient(clientv3.Config{
		Endpoints:   x.Endpoints,
		DialTimeout: 0, // 使用 ctx 控制
	})
	if err != nil {
		// 失败不缓存，下次 Take 可重试
		return nil, err
	}
	x.client = client
	x.initialized = true
	return x.client, nil
}

func (x *EtcdConnectionManager) Return(ctx context.Context, connection *clientv3.Client) error {
	return nil
}

func (x *EtcdConnectionManager) Shutdown(ctx context.Context) error {
	x.initMu.Lock()
	client := x.client
	x.initMu.Unlock()
	if client != nil {
		return client.Close()
	}
	return nil
}
