package etcd_storage

import (
	"context"
	storage_test_helper "github.com/storage-lock/go-storage-test-helper"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestEtcdStorage(t *testing.T) {
	envName := "STORAGE_LOCK_ETCD_ENDPOINTS"
	endpoints := os.Getenv(envName)
	if endpoints == "" {
		t.Skip("STORAGE_LOCK_ETCD_ENDPOINTS not set, skipping etcd test")
	}

	connectionManager := NewEtcdConnectionManagerFromEndpoints([]string{endpoints})
	s, err := NewEtcdStorage(context.Background(), &EtcdStorageOptions{
		ConnectionManager: connectionManager,
		KeyPrefix:         storage_test_helper.TestTableName,
	})
	assert.Nil(t, err)
	storage_test_helper.TestStorage(t, s)
}
