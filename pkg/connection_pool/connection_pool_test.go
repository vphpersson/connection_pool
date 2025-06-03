package connection_pool_test

import (
	"errors"
	"github.com/vphpersson/connection_pool/pkg/connection_pool"
	"net"
	"sync"
	"testing"
	"time"
)

type mockConnection struct {
	isClosed bool
	mu       sync.Mutex
}

func (mc *mockConnection) Read(_ []byte) (int, error)  { return 0, nil }
func (mc *mockConnection) Write(_ []byte) (int, error) { return 0, nil }
func (mc *mockConnection) Close() error {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if mc.isClosed {
		return errors.New("connection already closed")
	}
	mc.isClosed = true
	return nil
}
func (mc *mockConnection) LocalAddr() net.Addr                { return nil }
func (mc *mockConnection) RemoteAddr() net.Addr               { return nil }
func (mc *mockConnection) SetDeadline(_ time.Time) error      { return nil }
func (mc *mockConnection) SetReadDeadline(_ time.Time) error  { return nil }
func (mc *mockConnection) SetWriteDeadline(_ time.Time) error { return nil }

func newMockConnection() (*mockConnection, error) {
	return &mockConnection{}, nil
}

func TestConnectionPool_New(t *testing.T) {
	t.Parallel()

	pool := connection_pool.New(func() (*mockConnection, error) {
		return newMockConnection()
	})

	if pool == nil {
		t.Fatal("expected pool to be initialized, got nil")
	}

	if pool.MaxNumConnections != 5 {
		t.Fatalf("expected MaxNumConnections to be 5, got %d", pool.MaxNumConnections)
	}
}

func TestConnectionPool_GetPut(t *testing.T) {
	t.Parallel()

	pool := connection_pool.New(func() (*mockConnection, error) {
		return newMockConnection()
	})

	// Get a connection from the pool
	conn, err := pool.Get()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if conn == nil {
		t.Fatal("expected a valid connection, got nil")
	}

	// Put the connection back into the pool without an error
	pool.Put(t.Context(), conn, nil)

	if pool.Len() != 1 {
		t.Fatalf("expected pool length to be 1, got %d", pool.Len())
	}
}

func TestConnectionPool_MaxConnections(t *testing.T) {
	t.Parallel()

	pool := connection_pool.New(func() (*mockConnection, error) {
		return newMockConnection()
	})
	pool.MaxNumConnections = 2

	// Get the first connection
	conn1, err := pool.Get()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if conn1 == nil {
		t.Fatal("expected a valid connection, got nil")
	}

	// Get the second connection
	conn2, err := pool.Get()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if conn2 == nil {
		t.Fatal("expected a valid connection, got nil")
	}

	// Attempt to get a third connection (should block or not succeed immediately)
	done := make(chan struct{})
	go func() {
		_, _ = pool.Get() // Will block
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("Expected to block on third connection request")
	case <-time.After(100 * time.Millisecond):
		// Blocked due to max connections (expected behavior)
	}

	pool.Put(t.Context(), conn1, nil)

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Blocked get did not succeed after freeing up a connection")
	}
}

func TestConnectionPool_Close(t *testing.T) {
	t.Parallel()

	pool := connection_pool.New(func() (*mockConnection, error) {
		return newMockConnection()
	})

	conn, err := pool.Get()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	pool.Put(t.Context(), conn, nil)

	if err = pool.Close(); err != nil {
		t.Fatalf("unexpected error during pool close: %v", err)
	}

	if pool.Len() != 0 {
		t.Fatalf("expected pool to be empty after close, but got length %d", pool.Len())
	}
}

func TestConnectionPool_ErrorOnPut(t *testing.T) {
	t.Parallel()

	pool := connection_pool.New(func() (*mockConnection, error) {
		return newMockConnection()
	})

	conn, err := pool.Get()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	pool.Put(t.Context(), conn, errors.New("mock error"))

	// Verify the connection is not added back to the pool
	if pool.Len() != 0 {
		t.Fatalf("expected pool length to be 0 after error, but got %d", pool.Len())
	}
}

func TestConnectionPool_Get_MakeConnectionFails(t *testing.T) {
	t.Parallel()

	pool := connection_pool.New(func() (*mockConnection, error) {
		return nil, errors.New("connection creation failed")
	})

	conn, err := pool.Get()
	if err == nil {
		t.Fatal("expected error when MakeConnection fails, got nil")
	}
	if conn != nil {
		t.Fatal("expected nil connection when MakeConnection fails")
	}
}

func TestConnectionPool_CloseEmptyPool(t *testing.T) {
	t.Parallel()

	pool := connection_pool.New(func() (*mockConnection, error) {
		return newMockConnection()
	})

	// Close without any connections should not return an error
	err := pool.Close()
	if err != nil {
		t.Fatalf("expected no error closing empty pool, got %v", err)
	}
}
