package gorethink

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/stretchr/testify/assert"
)

// Mocking is based on the amazing package github.com/stretchr/testify

type MockQuery struct {
	parent *Mock

	// Holds the query and term
	Query Query

	// Holds the JSON representation of query
	BuiltQuery []byte

	// Holds the response that should be returned when this method is called.
	Response interface{}

	// Holds the error that should be returned when this method is called.
	Error error

	// The number of times to return the return arguments when setting
	// expectations. 0 means to always return the value.
	Repeatability int

	// Holds a channel that will be used to block the Return until it either
	// recieves a message or is closed. nil means it returns immediately.
	WaitFor <-chan time.Time

	// Amount of times this call has been called
	count int
}

func newMockQuery(parent *Mock, q Query) *MockQuery {
	// Build and marshal term
	builtQuery, err := json.Marshal(q.build())
	if err != nil {
		panic(fmt.Sprintf("Failed to build query: %s", err))
	}

	return &MockQuery{
		parent:        parent,
		Query:         q,
		BuiltQuery:    builtQuery,
		Response:      make([]interface{}, 0),
		Repeatability: 0,
		WaitFor:       nil,
	}
}

func newMockQueryFromTerm(parent *Mock, t Term, opts map[string]interface{}) *MockQuery {
	q, err := parent.newQuery(t, opts)
	if err != nil {
		panic(fmt.Sprintf("Failed to build query: %s", err))
	}

	return newMockQuery(parent, q)
}

func (mq *MockQuery) lock() {
	mq.parent.mu.Lock()
}

func (mq *MockQuery) unlock() {
	mq.parent.mu.Unlock()
}

// Return specifies the return arguments for the expectation.
//
//    Mock.On("DoSomething").Return(nil, errors.New("failed"))
func (mq *MockQuery) Return(response interface{}, err error) *MockQuery {
	mq.lock()
	defer mq.unlock()

	mq.Response = response
	mq.Error = err

	return mq
}

// Once indicates that that the mock should only return the value once.
//
//    Mock.On("MyMethod", arg1, arg2).Return(returnArg1, returnArg2).Once()
func (mq *MockQuery) Once() *MockQuery {
	return mq.Times(1)
}

// Twice indicates that that the mock should only return the value twice.
//
//    Mock.On("MyMethod", arg1, arg2).Return(returnArg1, returnArg2).Twice()
func (mq *MockQuery) Twice() *MockQuery {
	return mq.Times(2)
}

// Times indicates that that the mock should only return the indicated number
// of times.
//
//    Mock.On("MyMethod", arg1, arg2).Return(returnArg1, returnArg2).Times(5)
func (mq *MockQuery) Times(i int) *MockQuery {
	mq.lock()
	defer mq.unlock()
	mq.Repeatability = i
	return mq
}

// WaitUntil sets the channel that will block the mock's return until its closed
// or a message is received.
//
//    Mock.On("MyMethod", arg1, arg2).WaitUntil(time.After(time.Second))
func (mq *MockQuery) WaitUntil(w <-chan time.Time) *MockQuery {
	mq.lock()
	defer mq.unlock()
	mq.WaitFor = w
	return mq
}

// After sets how long to block until the call returns
//
//    Mock.On("MyMethod", arg1, arg2).After(time.Second)
func (mq *MockQuery) After(d time.Duration) *MockQuery {
	return mq.WaitUntil(time.After(d))
}

// On chains a new expectation description onto the mocked interface. This
// allows syntax like.
//
//    Mock.
//       On("MyMethod", 1).Return(nil).
//       On("MyOtherMethod", 'a', 'b', 'c').Return(errors.New("Some Error"))
func (mq *MockQuery) On(t Term) *MockQuery {
	return mq.parent.On(t)
}

type Mock struct {
	mu   sync.Mutex
	opts ConnectOpts

	ExpectedQueries []*MockQuery
	Queries         []MockQuery
}

func NewMock(opts ...ConnectOpts) *Mock {
	m := &Mock{
		ExpectedQueries: make([]*MockQuery, 0),
		Queries:         make([]MockQuery, 0),
	}

	if len(opts) > 0 {
		m.opts = opts[0]
	}

	return m
}

func (m *Mock) On(t Term, opts ...map[string]interface{}) *MockQuery {
	var qopts map[string]interface{}
	if len(opts) > 0 {
		qopts = opts[0]
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	mq := newMockQueryFromTerm(m, t, qopts)
	m.ExpectedQueries = append(m.ExpectedQueries, mq)
	return mq
}

func (m *Mock) IsConnected() bool {
	return true
}

func (m *Mock) Query(q Query) (*Cursor, error) {
	found, query := m.findExpectedQuery(q)

	if found < 0 {
		panic(fmt.Sprintf("gorethink: mock: This query was unexpected:\n\t\t%s\n\tat: %s", q.Term.String(), assert.CallerInfo()))
	} else {
		m.mu.Lock()
		switch {
		case query.Repeatability == 1:
			query.Repeatability = -1
			query.count++

		case query.Repeatability > 1:
			query.Repeatability--
			query.count++

		case query.Repeatability == 0:
			query.count++
		}
		m.mu.Unlock()
	}

	// add the query
	m.mu.Lock()
	m.Queries = append(m.Queries, *newMockQuery(m, q))
	m.mu.Unlock()

	// block if specified
	if query.WaitFor != nil {
		<-query.WaitFor
	}

	// Return error without building cursor if non-nil
	if query.Error != nil {
		return nil, query.Error
	}

	// Build cursor and return
	c := newCursor(nil, "", query.Query.Token, query.Query.Term, query.Query.Opts)
	c.buffer = append(c.buffer, query.Response)
	c.finished = true
	c.fetching = false
	c.isAtom = true

	return c, nil
}

func (m *Mock) Exec(q Query) error {
	_, err := m.Query(q)

	return err
}

func (m *Mock) newQuery(t Term, opts map[string]interface{}) (Query, error) {
	return newQuery(t, opts, &m.opts)
}

func (m *Mock) findExpectedQuery(q Query) (int, *MockQuery) {
	// Build and marshal query
	builtQuery, err := json.Marshal(q.build())
	if err != nil {
		panic(fmt.Sprintf("Failed to build query: %s", err))
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for i, query := range m.ExpectedQueries {
		if bytes.Equal(query.BuiltQuery, builtQuery) && query.Repeatability > -1 {
			return i, query
		}
	}

	return -1, nil
}
