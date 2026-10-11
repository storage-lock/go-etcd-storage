package etcd_storage

import (
	"github.com/storage-lock/go-storage"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdStorageOptions etcd 的存储选项
type EtcdStorageOptions struct {

	// 获取连接
	ConnectionManager storage.ConnectionManager[*clientv3.Client]

	// KeyPrefix etcd 中 key 的前缀，用于隔离不同的锁实例
	// 实际的 key 格式为：<KeyPrefix>/lock/<lockId>
	KeyPrefix string
}

func NewEtcdStorageOptions() *EtcdStorageOptions {
	return &EtcdStorageOptions{
		KeyPrefix: storage.DefaultStorageTableName,
	}
}

func NewEtcdStorageOptionsWithEndpoints(endpoints []string) *EtcdStorageOptions {
	return NewEtcdStorageOptions().SetConnectionManager(NewEtcdConnectionManagerFromEndpoints(endpoints))
}

func (x *EtcdStorageOptions) SetKeyPrefix(keyPrefix string) *EtcdStorageOptions {
	x.KeyPrefix = keyPrefix
	return x
}

func (x *EtcdStorageOptions) SetConnectionManager(connectionManager storage.ConnectionManager[*clientv3.Client]) *EtcdStorageOptions {
	x.ConnectionManager = connectionManager
	return x
}

func (x *EtcdStorageOptions) Check() error {
	if x.KeyPrefix == "" {
		return ErrKeyPrefixEmpty
	}
	if x.ConnectionManager == nil {
		return ErrConnectionManagerNil
	}
	return nil
}
