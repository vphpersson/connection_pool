package connection_pool

import (
	"container/list"
	"context"
	"fmt"
	motmedelContext "github.com/Motmedel/utils_go/pkg/context"
	motmedelErrors "github.com/Motmedel/utils_go/pkg/errors"
	"log/slog"
	"net"
	"sync"
)

type ConnectionPool[T net.Conn] struct {
	MaxNumConnections int
	MinNumConnections int
	MakeConnection    func() (*T, error)

	numActiveConnections int
	cond                 *sync.Cond
	connections          *list.List
	mutex                *sync.Mutex
}

func New[T net.Conn](fn func() (*T, error)) *ConnectionPool[T] {
	mutex := new(sync.Mutex)
	return &ConnectionPool[T]{
		MaxNumConnections:    5,
		MakeConnection:       fn,
		mutex:                mutex,
		connections:          list.New(),
		cond:                 sync.NewCond(mutex),
	}
}

func (pool *ConnectionPool[T]) Get() (*T, error) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	for pool.connections.Len() == 0 && pool.numActiveConnections >= pool.MaxNumConnections {
		pool.cond.Wait()
	}

	if pool.connections.Len() > 0 {
		return pool.connections.Remove(pool.connections.Front()).(*T), nil
	}

	connection, err := pool.MakeConnection()
	if err != nil {
		return nil, fmt.Errorf("make connection: %w", err)
	}

	pool.numActiveConnections++

	return connection, nil
}

func (pool *ConnectionPool[T]) Put(ctx context.Context, connection *T, err error) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	if err != nil {
		if connection != nil {
			if err := (*connection).Close(); err != nil {
				slog.WarnContext(
					motmedelContext.WithErrorContextValue(
						ctx,
						motmedelErrors.New(fmt.Errorf("connection close: %w", err), connection),
					),
					"An error occurred when closing a connection.",
				)
			}
			pool.numActiveConnections--
		}
	} else {
		pool.connections.PushBack(connection)
	}

	pool.cond.Signal()
}

func (pool *ConnectionPool[T]) Close(ctx context.Context) {
	connections := pool.connections
	if connections == nil || connections.Len() == 0 {
		return
	}

	for element := connections.Front(); element != nil; element = element.Next() {
		if connection, ok := element.Value.(net.Conn); ok {
			if err := connection.Close(); err != nil {
				slog.WarnContext(
					motmedelContext.WithErrorContextValue(
						ctx,
						motmedelErrors.New(fmt.Errorf("connection close: %w", err), connection),
					),
					"An error occurred when closing a connection.",
				)
			}
		}
	}
}

func (pool *ConnectionPool[T]) Len() int {
	return pool.connections.Len()
}
