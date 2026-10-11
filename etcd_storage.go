package etcd_storage

import (
	"context"
	"encoding/json"
	"fmt"
	go_iterator "github.com/golang-infrastructure/go-iterator"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	clientv3 "go.etcd.io/etcd/client/v3"
	"strconv"
	"strings"
	"time"
)

// ------------------------------------------------ ---------------------------------------------------------------------

// EtcdStorage 基于 etcd 的存储引擎实现
//
// etcd 是分布式共识 KV 存储，原生支持 CAS（Txn API 的 Compare+Op），
// 是分布式锁最自然的存储后端之一。CAS 由 etcd 服务端 Raft 共识保证原子性，
// 无需 Lua 脚本或进程内互斥锁。
type EtcdStorage struct {
	options *EtcdStorageOptions
	client  *clientv3.Client
}

var _ storage.Storage = &EtcdStorage{}

// NewEtcdStorage 创建一个基于 etcd 的存储引擎
func NewEtcdStorage(ctx context.Context, options *EtcdStorageOptions) (*EtcdStorage, error) {

	if err := options.Check(); err != nil {
		return nil, err
	}

	s := &EtcdStorage{
		options: options,
	}

	err := s.Init(ctx)
	if err != nil {
		return nil, err
	}

	return s, nil
}

const EtcdStorageName = "etcd-storage"

// etcdBeforeNotify 在向监听方发送前调用，默认空操作，测试用来卡住发送。
var etcdBeforeNotify = func(context.Context) {}

func (x *EtcdStorage) GetName() string {
	return EtcdStorageName
}

// Capabilities 声明 etcd 存储支持的能力
// etcd 通过 Txn API 提供原生 CAS，通过 revision 提供有序时间语义，
// 且原生支持 Watch 能力。
func (x *EtcdStorage) Capabilities() []storage.StorageCapability {
	return []storage.StorageCapability{
		storage.CapabilityCAS,
		storage.CapabilityReliableTime,
		storage.CapabilityAtomicDelete,
		storage.CapabilityWatch,
	}
}

func (x *EtcdStorage) Init(ctx context.Context) error {
	client, err := x.options.ConnectionManager.Take(ctx)
	if err != nil {
		return err
	}
	x.client = client
	return nil
}

// lockKey 返回 etcd 中存储锁信息的 key
func (x *EtcdStorage) lockKey(lockId string) string {
	return fmt.Sprintf("%s/lock/%s", x.options.KeyPrefix, lockId)
}

// lockPrefix 返回锁 key 的前缀，用于 List 扫描
func (x *EtcdStorage) lockPrefix() string {
	return fmt.Sprintf("%s/lock/", x.options.KeyPrefix)
}

// etcdLockValue 存储在 etcd value 中的结构
type etcdLockValue struct {
	Version string `json:"version"`
	Json    string `json:"json"`
}

// CreateWithVersion 原子创建锁记录
// etcd Txn API：如果 key 不存在（CreateRevision==0）则 Put，否则返回 VersionMiss
func (x *EtcdStorage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, lockInformation *storage.LockInformation) error {
	key := x.lockKey(lockId)
	value := &etcdLockValue{
		Version: strconv.FormatUint(uint64(version), 10),
		Json:    lockInformation.ToJsonString(),
	}
	// etcdLockValue 只含字符串，序列化不会失败。
	valueBytes, _ := json.Marshal(value)

	// Txn: 如果 key 的 CreateRevision == 0（不存在），则 Put
	txn := x.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(valueBytes))).
		Else()

	resp, err := txn.Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return storage_lock.ErrVersionMiss
	}
	return nil
}

// UpdateWithVersion 原子更新锁记录
// etcd Txn API：如果当前 value 中的 version 匹配，则 Put 新值
func (x *EtcdStorage) UpdateWithVersion(ctx context.Context, lockId string, exceptedVersion, newVersion storage.Version, lockInformation *storage.LockInformation) error {
	key := x.lockKey(lockId)

	// 先读取当前值获取 version
	getResp, err := x.client.Get(ctx, key)
	if err != nil {
		return err
	}
	// 注意：key 不存在时映射为 ErrVersionMiss 而不是 ErrLockNotFound——core 的
	// lockExpired/rlockReclaim 等抢占路径只把 ErrVersionMiss 当作"CAS 失败，可重试"，
	// 其它错误会被当作硬错误直接返回。若真正的持有者恰好在这中间 DeleteWithVersion
	// 真删了记录（与本次抢占竞态），"记录不存在"和"版本不匹配"对调用方而言是同一件事，
	// 期望的前提已经不成立，应该重试而不是硬失败。
	if len(getResp.Kvs) == 0 {
		return storage_lock.ErrVersionMiss
	}

	// 解析当前版本号
	currentValue := &etcdLockValue{}
	if err := json.Unmarshal(getResp.Kvs[0].Value, currentValue); err != nil {
		return err
	}
	currentVersion, err := strconv.ParseUint(currentValue.Version, 10, 64)
	if err != nil {
		return err
	}
	if storage.Version(currentVersion) != exceptedVersion {
		return storage_lock.ErrVersionMiss
	}

	// 构建新值
	newValue := &etcdLockValue{
		Version: strconv.FormatUint(uint64(newVersion), 10),
		Json:    lockInformation.ToJsonString(),
	}
	// etcdLockValue 只含字符串，序列化不会失败。
	newValueBytes, _ := json.Marshal(newValue)

	// Txn: 如果 value 中的 version 仍为 exceptedVersion，则 Put
	// 使用 ModRevision 作为乐观锁保护并发更新
	txn := x.client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", getResp.Kvs[0].ModRevision)).
		Then(clientv3.OpPut(key, string(newValueBytes))).
		Else()

	resp, err := txn.Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return storage_lock.ErrVersionMiss
	}
	return nil
}

// DeleteWithVersion 原子删除锁记录
// etcd Txn API：如果当前 value 中的 version 匹配，则 Delete
func (x *EtcdStorage) DeleteWithVersion(ctx context.Context, lockId string, exceptedVersion storage.Version, lockInformation *storage.LockInformation) error {
	key := x.lockKey(lockId)

	// 先读取当前值
	getResp, err := x.client.Get(ctx, key)
	if err != nil {
		return err
	}
	if len(getResp.Kvs) == 0 {
		return storage_lock.ErrVersionMiss
	}

	// 解析当前版本号
	currentValue := &etcdLockValue{}
	if err := json.Unmarshal(getResp.Kvs[0].Value, currentValue); err != nil {
		return err
	}
	currentVersion, err := strconv.ParseUint(currentValue.Version, 10, 64)
	if err != nil {
		return err
	}
	if storage.Version(currentVersion) != exceptedVersion {
		return storage_lock.ErrVersionMiss
	}

	// Txn: 如果 ModRevision 未变（没人并发修改），则 Delete
	txn := x.client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", getResp.Kvs[0].ModRevision)).
		Then(clientv3.OpDelete(key)).
		Else()

	resp, err := txn.Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return storage_lock.ErrVersionMiss
	}
	return nil
}

// Get 获取锁信息
func (x *EtcdStorage) Get(ctx context.Context, lockId string) (string, error) {
	key := x.lockKey(lockId)
	resp, err := x.client.Get(ctx, key)
	if err != nil {
		return "", err
	}
	if len(resp.Kvs) == 0 {
		return "", storage_lock.ErrLockNotFound
	}

	value := &etcdLockValue{}
	if err := json.Unmarshal(resp.Kvs[0].Value, value); err != nil {
		return "", err
	}
	return value.Json, nil
}

// GetTime 获取 etcd 服务端时间
// etcd 没有直接的时间 API，但可以使用 Get 操作的时间戳近似
// 或使用本地时间（etcd 集群通常有 NTP 同步）
// 注意：这里返回本地时间，因为 etcd 没有直接的服务端时间 API
// 在生产环境中，可注入 go-ntp-time-provider 替代
func (x *EtcdStorage) GetTime(ctx context.Context) (time.Time, error) {
	return time.Now(), nil
}

// Close 关闭存储引擎
// 连接由 ConnectionManager 管理，Storage 只清空引用
func (x *EtcdStorage) Close(ctx context.Context) error {
	x.client = nil
	return nil
}

// List 列出当前所有的锁信息
// 使用 etcd 的前缀查询功能
func (x *EtcdStorage) List(ctx context.Context) (go_iterator.Iterator[*storage.LockInformation], error) {
	prefix := x.lockPrefix()
	resp, err := x.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	slice := make([]*storage.LockInformation, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		// 跳过非 lock key（可能存在前缀冲突的其它 key）
		key := string(kv.Key)
		// key 格式: <prefix>/lock/<lockId>
		if !strings.Contains(key, "/lock/") {
			continue
		}

		value := &etcdLockValue{}
		if err := json.Unmarshal(kv.Value, value); err != nil {
			continue
		}

		info := &storage.LockInformation{}
		if err := json.Unmarshal([]byte(value.Json), info); err != nil {
			continue
		}
		slice = append(slice, info)
	}
	return go_iterator.FromSlice(slice), nil
}

// Watch 监听指定锁的状态变更。
func (x *EtcdStorage) Watch(ctx context.Context, lockId string) (<-chan *storage.LockInformation, error) {
	key := x.lockKey(lockId)
	ch := make(chan *storage.LockInformation)

	go func() {
		defer close(ch)
		watchChan := x.client.Watch(ctx, key)

		for {
			select {
			case <-ctx.Done():
				return
			case resp, ok := <-watchChan:
				if !ok {
					return
				}
				for _, event := range resp.Events {
					if event == nil || event.Kv == nil {
						continue
					}
					value := &etcdLockValue{}
					if err := json.Unmarshal(event.Kv.Value, value); err != nil {
						continue
					}
					info := &storage.LockInformation{}
					if err := json.Unmarshal([]byte(value.Json), info); err != nil {
						continue
					}
					etcdBeforeNotify(ctx)
					select {
					case <-ctx.Done():
						return
					case ch <- info:
					}
				}
			}
		}
	}()

	return ch, nil
}

// ------------------------------------------------ ---------------------------------------------------------------------
