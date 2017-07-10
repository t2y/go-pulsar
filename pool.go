package pulsar

import (
	"fmt"
	"sync"

	"github.com/pkg/errors"
)

const (
	defaultMinConnNum = 2
	defaultMaxConnNum = 20
)

var ( // Errors
	ErrExistsConnInPool = errors.New("same connection exists in pool")
	ErrNoAvailablePool  = errors.New("no available connection in pool")
	ErrReachMaxConn     = errors.New("connections in pool reach a maximum number")
)

type PoolStatus struct {
	availableNum int
	inUseNum     int
	inUseIds     []string
}

func (p *PoolStatus) String() (s string) {
	tmpl := "pool status, available: %d, in use: %d, in use ids: %s"
	s = fmt.Sprintf(tmpl, p.availableNum, p.inUseNum, p.inUseIds)
	return
}

type ConnPool struct {
	minConnNum int
	maxConnNum int

	mu        sync.Mutex
	available AsyncConns
	inUse     map[string]*AsyncConn
}

func (p *ConnPool) Get() (c *AsyncConn, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.available) == 0 {
		err = ErrNoAvailablePool
		return
	}

	index := 0
	c = p.available[index]
	id := c.GetID()

	p.available[index] = p.available[len(p.available)-1]
	p.available[len(p.available)-1] = nil
	p.available = p.available[:len(p.available)-1]

	p.inUse[id] = c
	return
}

func (p *ConnPool) Put(c *AsyncConn) (err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	id := c.GetID()
	for _, conn := range p.available {
		if conn != nil {
			if conn.GetID() == id {
				err = ErrExistsConnInPool
				return
			}
		}
	}

	if _, ok := p.inUse[id]; ok {
		delete(p.inUse, id) // given connection was released
	}

	if len(p.available)+len(p.inUse) == p.maxConnNum {
		err = ErrReachMaxConn
		return
	}

	p.available = append(p.available, c)
	return
}

func (p *ConnPool) Delete(c *AsyncConn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	id := c.GetID()
	if _, ok := p.inUse[id]; ok {
		delete(p.inUse, id)
	}
	return
}

func (p *ConnPool) GetStatus() (status *PoolStatus) {
	p.mu.Lock()
	idsLength := len(p.inUse)
	inUseIds := make([]string, 0, idsLength)
	for id := range p.inUse {
		inUseIds = append(inUseIds, id)
	}

	status = &PoolStatus{
		availableNum: len(p.available),
		inUseNum:     idsLength,
		inUseIds:     inUseIds,
	}
	p.mu.Unlock()
	return
}

func (p *ConnPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, conn := range p.available {
		if conn != nil {
			conn.Close()
		}
	}
	for _, conn := range p.inUse {
		if conn != nil {
			conn.Close()
		}
	}
}

func NewConnPool(c *Config) (p *ConnPool, err error) {
	available := make(AsyncConns, 0, c.MaxConnectionNum)
	inUse := make(map[string]*AsyncConn, c.MaxConnectionNum)

	for i := 0; i < c.MinConnectionNum; i++ {
		conn, e := NewConn(c)
		if e != nil {
			err = errors.Wrap(e, "failed to create tcp connection")
			return
		}

		cmd, e := NewCommandConnect(c, true)
		if err != nil {
			err = errors.Wrap(e, "failed to create connect command")
			return
		}

		asyncConn := NewAsyncConn(c, conn)
		if e := asyncConn.Connect(cmd); e != nil {
			err = errors.Wrap(e, "failed to send connect command")
			return
		}

		available = append(available, asyncConn)
	}

	p = &ConnPool{
		minConnNum: c.MinConnectionNum,
		maxConnNum: c.MaxConnectionNum,
		available:  available,
		inUse:      inUse,
	}
	return
}
