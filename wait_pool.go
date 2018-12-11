package redisc

import (
	"context"
	"time"

	"github.com/gomodule/redigo/redis"
	"golang.org/x/sync/semaphore"
)

// waitPool wrap the redis pool, make it protected by semaphore to limit the access
type waitPool struct {
	*redis.Pool
	sem       *semaphore.Weighted
	waitTime  time.Duration
	maxActive int
}

func newWaitPool(pool *redis.Pool, waitTimeMs int) *waitPool {
	return &waitPool{
		Pool:      pool,
		sem:       semaphore.NewWeighted(int64(pool.MaxActive)),
		waitTime:  time.Duration(waitTimeMs) * time.Millisecond,
		maxActive: pool.MaxActive,
	}
}

// getWait get connection from the pool.
// If pool is full (no available connection) it wait for the configured timeout,
// before returning invalid conn.
// (default behaviour is to simply return invalid conn when pool is full)
func (wp *waitPool) getWait() (redis.Conn, error) {
	var sem *semaphore.Weighted

	if wp.maxActive > 0 { // when maxActive == 0, we don't need the semaphore, because the pool also has no limitation
		ctx, cancel := context.WithTimeout(context.Background(), wp.waitTime)
		defer cancel()

		err := wp.sem.Acquire(ctx, 1)
		if err != nil {
			return nil, err
		}
		sem = wp.sem
	}

	conn := wp.Pool.Get()

	return &connSem{
		Conn: conn,
		sem:  sem,
	}, nil
}

// connSem is redis connection protected by semaphore
type connSem struct {
	sem *semaphore.Weighted
	redis.Conn
}

func (c *connSem) Close() error {
	c.Conn.Close()
	if c.sem != nil { // nil semaphore could happen when pool.max_active == 0
		c.sem.Release(1)
	}
	return nil
}
