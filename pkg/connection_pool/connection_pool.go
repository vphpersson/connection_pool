package connection_pool

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	motmedelContext "github.com/Motmedel/utils_go/pkg/context"
	motmedelErrors "github.com/Motmedel/utils_go/pkg/errors"
	connectionPoolErrors "github.com/vphpersson/connection_pool/pkg/errors"
	"log/slog"
	"net"
	"strings"
	"sync"
)

type ConnectionPool[T net.Conn] struct {
	MaxNumConnections int
	MinNumConnections int
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
				fmt.Errorf("%w (generic net.Conn)", motmedelErrors.ErrConversionNotOk),
				element,
			)
		}
		if net.Conn(connection) == nil {
			return zero, motmedelErrors.NewWithTrace(connectionPoolErrors.ErrNilConnection)
		}

		return connection, nil
	}

	connection, err := pool.MakeConnection()
	if err != nil {
		return zero, fmt.Errorf("make connection: %w", err)
	}
	if net.Conn(connection) == nil {
		return zero, motmedelErrors.NewWithTrace(connectionPoolErrors.ErrNilConnection)
	}

	pool.connections.PushBack(connection)
	pool.numActiveConnections++

	return connection, nil
}

func (pool *ConnectionPool[T]) Put(ctx context.Context, connection T, err error) {
	if net.Conn(connection) == nil {
		return
	}

	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	if err != nil {
		if err := connection.Close(); err != nil && !strings.HasSuffix(err.Error(), "use of closed network connection") && !errors.Is(err, net.ErrClosed) {
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
		pool.connections.PushBack(connection)
	}

	pool.condition.Signal()
}

func (pool *ConnectionPool[T]) Close() error {
	connections := pool.connections
	if connections == nil || connections.Len() == 0 {
		return nil
	}

	for element := connections.Front(); element != nil; element = element.Next() {
		elementValue := element.Value
		connection, ok := elementValue.(net.Conn)
		if !ok {
			return motmedelErrors.NewWithTrace(
				fmt.Errorf("%w (net.Conn)", motmedelErrors.ErrConversionNotOk),
				elementValue,
			)
		}
		if connection == nil {
			return motmedelErrors.NewWithTrace(connectionPoolErrors.ErrNilConnection)
		}

		if err := connection.Close(); err != nil {
			return motmedelErrors.New(fmt.Errorf("connection close: %w", err), connection)
		}
	}

	return nil
}

func (pool *ConnectionPool[T]) Len() int {
	return pool.connections.Len()
}
