package connection_pool

import (
	"container/list"
	"context"
	"fmt"
	motmedelContext "github.com/Motmedel/utils_go/pkg/context"
	motmedelErrors "github.com/Motmedel/utils_go/pkg/errors"
	connectionPoolErrors "github.com/vphpersson/connection_pool/pkg/errors"
	"io"
	"log/slog"
	"net"
	"sync"
)

type ConnectionPool[T io.Closer] struct {
	MaxNumConnections int
	MakeConnection    func() (T, error)

	numActiveConnections int
	condition            *sync.Cond
	connections          *list.List
	mutex                *sync.Mutex
}

func New[T net.Conn](fn func() (T, error)) *ConnectionPool[T] {
	mutex := new(sync.Mutex)
	return &ConnectionPool[T]{
		MaxNumConnections: 5,
		MakeConnection:    fn,
		mutex:             mutex,
		connections:       list.New(),
		condition:         sync.NewCond(mutex),
	}
}

func (pool *ConnectionPool[T]) Get() (T, error) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	for pool.connections.Len() == 0 && pool.numActiveConnections >= pool.MaxNumConnections {
		pool.condition.Wait()
	}

	var zero T

	if pool.connections.Len() > 0 {
		element := pool.connections.Remove(pool.connections.Front())
		connection, ok := element.(T)
		if !ok {
			return zero, motmedelErrors.NewWithTrace(
				fmt.Errorf("%w (generic io.Closer)", motmedelErrors.ErrConversionNotOk),
				element,
			)
		}
		if io.Closer(connection) == nil {
			return zero, motmedelErrors.NewWithTrace(connectionPoolErrors.ErrNilConnection)
		}

		return connection, nil
	}

	connection, err := pool.MakeConnection()
	if err != nil {
		return zero, fmt.Errorf("make connection: %w", err)
	}
	if io.Closer(connection) == nil {
		return zero, motmedelErrors.NewWithTrace(connectionPoolErrors.ErrNilConnection)
	}

	pool.numActiveConnections++

	return connection, nil
}

func (pool *ConnectionPool[T]) Put(ctx context.Context, connection T, err error) {
	if io.Closer(connection) == nil {
		return
	}

	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	if err != nil {
		if err := connection.Close(); err != nil && !motmedelErrors.IsClosedError(err) {
			slog.WarnContext(
				motmedelContext.WithErrorContextValue(
					ctx,
					motmedelErrors.NewWithTrace(fmt.Errorf("connection close: %w", err), connection),
				),
				"An error occurred when closing a connection.",
			)
		}
		pool.numActiveConnections--
	} else {
		pool.connections.PushFront(connection)
	}

	pool.condition.Signal()
}

func (pool *ConnectionPool[T]) Close() error {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	if pool.connections == nil || pool.connections.Len() == 0 {
		return nil
	}
	for element := pool.connections.Front(); element != nil; {
		connection, _ := element.Value.(io.Closer)

		next := element.Next()
		pool.connections.Remove(element)

		if connection != nil {
			if err := connection.Close(); err != nil {
				return motmedelErrors.NewWithTrace(fmt.Errorf("connection close: %w", err), connection)
			}
		}

		element = next
	}

	pool.connections = list.New()

	pool.numActiveConnections = 0

	return nil
}

func (pool *ConnectionPool[T]) Len() int {
	return pool.connections.Len()
}