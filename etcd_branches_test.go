package etcd_storage

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	"github.com/stretchr/testify/assert"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type fakeTxn struct {
	resp *clientv3.TxnResponse
	err  error
}

func (t *fakeTxn) If(...clientv3.Cmp) clientv3.Txn  { return t }
func (t *fakeTxn) Then(...clientv3.Op) clientv3.Txn { return t }
func (t *fakeTxn) Else(...clientv3.Op) clientv3.Txn { return t }
func (t *fakeTxn) Commit() (*clientv3.TxnResponse, error) {
	return t.resp, t.err
}

type fakeKV struct {
	getResp *clientv3.GetResponse
	getErr  error
	txn     clientv3.Txn
}

func (k *fakeKV) Put(context.Context, string, string, ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return nil, errors.New("unused")
}
func (k *fakeKV) Get(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return k.getResp, k.getErr
}
func (k *fakeKV) Delete(context.Context, string, ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return nil, errors.New("unused")
}
func (k *fakeKV) Compact(context.Context, int64, ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	return nil, errors.New("unused")
}
func (k *fakeKV) Do(context.Context, clientv3.Op) (clientv3.OpResponse, error) {
	return clientv3.OpResponse{}, errors.New("unused")
}
func (k *fakeKV) Txn(context.Context) clientv3.Txn { return k.txn }

type fakeWatcher struct {
	ch clientv3.WatchChan
}

func (w *fakeWatcher) Watch(context.Context, string, ...clientv3.OpOption) clientv3.WatchChan {
	return w.ch
}
func (w *fakeWatcher) RequestProgress(context.Context) error { return nil }
func (w *fakeWatcher) Close() error                          { return nil }

type takeErrorManager struct{}

func (takeErrorManager) Name() string { return "take-error" }
func (takeErrorManager) Take(context.Context) (*clientv3.Client, error) {
	return nil, errors.New("take failed")
}
func (takeErrorManager) Return(context.Context, *clientv3.Client) error { return nil }
func (takeErrorManager) Shutdown(context.Context) error                 { return nil }

func newFakeClient(kv clientv3.KV, watcher clientv3.Watcher) *clientv3.Client {
	client := clientv3.NewCtxClient(context.Background())
	client.KV = kv
	client.Watcher = watcher
	return client
}

func newTestEtcd(t *testing.T, kv clientv3.KV, watcher clientv3.Watcher) *EtcdStorage {
	t.Helper()
	s, err := NewEtcdStorage(context.Background(), &EtcdStorageOptions{
		ConnectionManager: NewEtcdConnectionManagerFromClient(newFakeClient(kv, watcher)),
		KeyPrefix:         "locks",
	})
	assert.NoError(t, err)
	return s
}

func kvOf(key string, mod int64, raw []byte) *clientv3.GetResponse {
	return (*clientv3.GetResponse)(&pb.RangeResponse{Kvs: []*mvccpb.KeyValue{{
		Key:         []byte(key),
		Value:       raw,
		ModRevision: mod,
	}}})
}

func encodeEtcd(version, jsonText string) []byte {
	raw, _ := json.Marshal(&etcdLockValue{Version: version, Json: jsonText})
	return raw
}

func lockJSON(owner string) string {
	return (&storage.LockInformation{LockId: "id", OwnerId: owner}).ToJsonString()
}

func txnResp(ok bool) *clientv3.TxnResponse {
	return (*clientv3.TxnResponse)(&pb.TxnResponse{Succeeded: ok})
}

func TestEtcdConnectionManager(t *testing.T) {
	preset := clientv3.NewCtxClient(context.Background())
	fromClient := NewEtcdConnectionManagerFromClient(preset)
	assert.Equal(t, EtcdConnectionManagerName, fromClient.Name())
	got, err := fromClient.Take(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, preset, got)
	assert.NoError(t, fromClient.Return(context.Background(), got))
	assert.Error(t, fromClient.Shutdown(context.Background()))

	empty := &EtcdConnectionManager{}
	assert.NoError(t, empty.Shutdown(context.Background()))

	fromEndpoints := NewEtcdConnectionManagerFromEndpoints(nil)
	_, err = fromEndpoints.Take(context.Background())
	assert.ErrorIs(t, err, clientv3.ErrNoAvailableEndpoints)
	opened := clientv3.NewCtxClient(context.Background())
	fromEndpoints.client = opened
	fromEndpoints.initialized = true
	got, err = fromEndpoints.Take(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, opened, got)

	orig := etcdNewClient
	defer func() { etcdNewClient = orig }()
	etcdNewClient = func(clientv3.Config) (*clientv3.Client, error) {
		return nil, errors.New("dial failed")
	}
	retry := NewEtcdConnectionManagerFromEndpoints([]string{"http://127.0.0.1:1"})
	_, err = retry.Take(context.Background())
	assert.EqualError(t, err, "dial failed")
	assert.False(t, retry.initialized)
	etcdNewClient = func(clientv3.Config) (*clientv3.Client, error) {
		return clientv3.NewCtxClient(context.Background()), nil
	}
	got, err = retry.Take(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, got)
	again, err := retry.Take(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, got, again)
}

func TestEtcdStorage_OptionsAndLifecycle(t *testing.T) {
	assert.Panics(t, func() { _, _ = NewEtcdStorage(context.Background(), nil) })
	_, err := NewEtcdStorage(context.Background(), NewEtcdStorageOptions())
	assert.ErrorIs(t, err, ErrConnectionManagerNil)
	opts := NewEtcdStorageOptionsWithEndpoints([]string{"http://127.0.0.1:1"}).SetKeyPrefix("")
	assert.ErrorIs(t, opts.Check(), ErrKeyPrefixEmpty)
	opts.SetKeyPrefix("locks")
	assert.NoError(t, opts.Check())

	_, err = NewEtcdStorage(context.Background(), &EtcdStorageOptions{ConnectionManager: takeErrorManager{}, KeyPrefix: "locks"})
	assert.EqualError(t, err, "take failed")

	s := newTestEtcd(t, &fakeKV{}, &fakeWatcher{})
	assert.Equal(t, EtcdStorageName, s.GetName())
	assert.Len(t, s.Capabilities(), 4)
	assert.Equal(t, "locks/lock/id", s.lockKey("id"))
	assert.Equal(t, "locks/lock/", s.lockPrefix())
	now, err := s.GetTime(context.Background())
	assert.NoError(t, err)
	assert.WithinDuration(t, time.Now(), now, time.Second)
	assert.NoError(t, s.Close(context.Background()))
}

func TestEtcdStorage_CreateWithVersion(t *testing.T) {
	kv := &fakeKV{txn: &fakeTxn{resp: txnResp(true)}}
	s := newTestEtcd(t, kv, &fakeWatcher{})
	info := &storage.LockInformation{OwnerId: "owner"}
	ctx := context.Background()
	assert.NoError(t, s.CreateWithVersion(ctx, "id", 1, info))

	kv.txn = &fakeTxn{resp: txnResp(false)}
	assert.ErrorIs(t, s.CreateWithVersion(ctx, "id", 1, info), storage_lock.ErrVersionMiss)

	kv.txn = &fakeTxn{err: errors.New("commit failed")}
	assert.EqualError(t, s.CreateWithVersion(ctx, "id", 1, info), "commit failed")
}

func TestEtcdStorage_UpdateWithVersion(t *testing.T) {
	kv := &fakeKV{}
	s := newTestEtcd(t, kv, &fakeWatcher{})
	info := &storage.LockInformation{OwnerId: "owner"}
	ctx := context.Background()

	kv.getErr = errors.New("get failed")
	assert.EqualError(t, s.UpdateWithVersion(ctx, "id", 1, 2, info), "get failed")

	kv.getErr = nil
	kv.getResp = (*clientv3.GetResponse)(&pb.RangeResponse{})
	assert.ErrorIs(t, s.UpdateWithVersion(ctx, "id", 1, 2, info), storage_lock.ErrVersionMiss)

	kv.getResp = kvOf("locks/lock/id", 7, []byte("bad"))
	assert.Error(t, s.UpdateWithVersion(ctx, "id", 1, 2, info))

	kv.getResp = kvOf("locks/lock/id", 7, encodeEtcd("nope", lockJSON("owner")))
	assert.Error(t, s.UpdateWithVersion(ctx, "id", 1, 2, info))

	kv.getResp = kvOf("locks/lock/id", 7, encodeEtcd("9", lockJSON("other")))
	assert.ErrorIs(t, s.UpdateWithVersion(ctx, "id", 1, 2, info), storage_lock.ErrVersionMiss)

	kv.getResp = kvOf("locks/lock/id", 7, encodeEtcd("1", lockJSON("owner")))
	kv.txn = &fakeTxn{err: errors.New("commit failed")}
	assert.EqualError(t, s.UpdateWithVersion(ctx, "id", 1, 2, info), "commit failed")

	kv.txn = &fakeTxn{resp: txnResp(false)}
	assert.ErrorIs(t, s.UpdateWithVersion(ctx, "id", 1, 2, info), storage_lock.ErrVersionMiss)

	kv.txn = &fakeTxn{resp: txnResp(true)}
	assert.NoError(t, s.UpdateWithVersion(ctx, "id", 1, 2, info))
}

func TestEtcdStorage_DeleteWithVersion(t *testing.T) {
	kv := &fakeKV{}
	s := newTestEtcd(t, kv, &fakeWatcher{})
	info := &storage.LockInformation{OwnerId: "owner"}
	ctx := context.Background()

	kv.getErr = errors.New("get failed")
	assert.EqualError(t, s.DeleteWithVersion(ctx, "id", 1, info), "get failed")

	kv.getErr = nil
	kv.getResp = (*clientv3.GetResponse)(&pb.RangeResponse{})
	assert.ErrorIs(t, s.DeleteWithVersion(ctx, "id", 1, info), storage_lock.ErrVersionMiss)

	kv.getResp = kvOf("locks/lock/id", 3, []byte("bad"))
	assert.Error(t, s.DeleteWithVersion(ctx, "id", 1, info))

	kv.getResp = kvOf("locks/lock/id", 3, encodeEtcd("bad", lockJSON("owner")))
	assert.Error(t, s.DeleteWithVersion(ctx, "id", 1, info))

	kv.getResp = kvOf("locks/lock/id", 3, encodeEtcd("4", lockJSON("owner")))
	assert.ErrorIs(t, s.DeleteWithVersion(ctx, "id", 1, info), storage_lock.ErrVersionMiss)

	kv.getResp = kvOf("locks/lock/id", 3, encodeEtcd("1", lockJSON("owner")))
	kv.txn = &fakeTxn{err: errors.New("commit failed")}
	assert.EqualError(t, s.DeleteWithVersion(ctx, "id", 1, info), "commit failed")

	kv.txn = &fakeTxn{resp: txnResp(false)}
	assert.ErrorIs(t, s.DeleteWithVersion(ctx, "id", 1, info), storage_lock.ErrVersionMiss)

	kv.txn = &fakeTxn{resp: txnResp(true)}
	assert.NoError(t, s.DeleteWithVersion(ctx, "id", 1, info))
}

func TestEtcdStorage_GetAndList(t *testing.T) {
	kv := &fakeKV{}
	s := newTestEtcd(t, kv, &fakeWatcher{})
	ctx := context.Background()

	kv.getErr = errors.New("get failed")
	_, err := s.Get(ctx, "id")
	assert.EqualError(t, err, "get failed")
	_, err = s.List(ctx)
	assert.EqualError(t, err, "get failed")

	kv.getErr = nil
	kv.getResp = (*clientv3.GetResponse)(&pb.RangeResponse{})
	_, err = s.Get(ctx, "id")
	assert.ErrorIs(t, err, storage_lock.ErrLockNotFound)

	kv.getResp = kvOf("locks/lock/id", 1, []byte("bad"))
	_, err = s.Get(ctx, "id")
	assert.Error(t, err)

	kv.getResp = kvOf("locks/lock/id", 1, encodeEtcd("1", lockJSON("owner")))
	got, err := s.Get(ctx, "id")
	assert.NoError(t, err)
	assert.Contains(t, got, "owner")

	kv.getResp = (*clientv3.GetResponse)(&pb.RangeResponse{Kvs: []*mvccpb.KeyValue{
		{Key: []byte("other"), Value: encodeEtcd("1", lockJSON("skip"))},
		{Key: []byte("locks/lock/bad-value"), Value: []byte("bad")},
		{Key: []byte("locks/lock/bad-json"), Value: encodeEtcd("1", "not-json")},
		{Key: []byte("locks/lock/ok"), Value: encodeEtcd("1", lockJSON("owner"))},
	}})
	listed, err := s.List(ctx)
	assert.NoError(t, err)
	count := 0
	for listed.Next() {
		count++
		assert.Equal(t, "owner", listed.Value().OwnerId)
	}
	assert.Equal(t, 1, count)
}

func TestEtcdStorage_Watch(t *testing.T) {
	ch := make(chan clientv3.WatchResponse, 1)
	ch <- clientv3.WatchResponse{Events: []*clientv3.Event{
		nil,
		{},
		{Kv: &mvccpb.KeyValue{Value: []byte("bad")}},
		{Kv: &mvccpb.KeyValue{Value: encodeEtcd("1", "not-json")}},
		{Kv: &mvccpb.KeyValue{Value: encodeEtcd("1", lockJSON("owner"))}},
	}}
	close(ch)
	s := newTestEtcd(t, &fakeKV{}, &fakeWatcher{ch: ch})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	out, err := s.Watch(ctx, "id")
	assert.NoError(t, err)
	select {
	case info := <-out:
		assert.Equal(t, "owner", info.OwnerId)
	case <-ctx.Done():
		t.Fatal("watch did not emit")
	}
	select {
	case _, ok := <-out:
		assert.False(t, ok)
	case <-ctx.Done():
		t.Fatal("watch did not close")
	}

	blocked := make(chan clientv3.WatchResponse)
	canceled, stop := context.WithCancel(context.Background())
	stop()
	watching := newTestEtcd(t, &fakeKV{}, &fakeWatcher{ch: blocked})
	out, err = watching.Watch(canceled, "id")
	assert.NoError(t, err)
	select {
	case _, ok := <-out:
		assert.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("canceled watch did not stop")
	}

	sendCtx, sendStop := context.WithCancel(context.Background())
	defer sendStop()
	entered := make(chan struct{})
	release := make(chan struct{})
	orig := etcdBeforeNotify
	defer func() { etcdBeforeNotify = orig }()
	etcdBeforeNotify = func(context.Context) {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
	}
	watcher := &blockingWatch{payload: encodeEtcd("1", lockJSON("blocked"))}
	sending := newTestEtcd(t, &fakeKV{}, watcher)
	out, err = sending.Watch(sendCtx, "id")
	assert.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("send did not block")
	}
	sendStop()
	close(release)
	select {
	case _, ok := <-out:
		assert.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("blocked send did not stop")
	}
}

// blockingWatch 在向无缓冲输出通道发送时阻塞，测试随后取消上下文。
type blockingWatch struct {
	payload []byte
}

func (w *blockingWatch) Watch(context.Context, string, ...clientv3.OpOption) clientv3.WatchChan {
	ch := make(chan clientv3.WatchResponse, 1)
	ch <- clientv3.WatchResponse{Events: []*clientv3.Event{{
		Kv: &mvccpb.KeyValue{Value: w.payload},
	}}}
	close(ch)
	return ch
}
func (w *blockingWatch) RequestProgress(context.Context) error { return nil }
func (w *blockingWatch) Close() error                          { return nil }
