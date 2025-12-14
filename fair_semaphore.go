package synx

// FairSemaphore is a counting semaphore that guarantees FIFO (First-In-First-Out) order.
//
// Standard Semaphores (like golang.org/x/sync/semaphore) generally optimize for throughput
// and may allow barging (new waiters stealing permits), which can lead to starvation
// or unfairness in specific workloads.
//
// FairSemaphore ensures that permits are strictly assigned to waiters in the order of arrival.
//
// Implementation:
// It uses a mutex-protected linked list of waiters and a `TicketLock` for the mutex itself
// to ensure even the internal lock acquisition is fair.
type FairSemaphore struct {
	_       noCopy
	mu      TicketLock
	permits int64
	head    *fairWaiter
	tail    *fairWaiter
}

type fairWaiter struct {
	next *fairWaiter
	n    int64
	sema uint32
}

func NewFairSemaphore(permits int64) *FairSemaphore {
	return &FairSemaphore{permits: permits}
}

func (s *FairSemaphore) Acquire(n int64) {
	if n <= 0 {
		return
	}
	s.mu.Lock()
	if s.head == nil && s.permits >= n {
		s.permits -= n
		s.mu.Unlock()
		return
	}
	w := &fairWaiter{n: n}
	if s.tail == nil {
		s.head = w
		s.tail = w
	} else {
		s.tail.next = w
		s.tail = w
	}
	s.mu.Unlock()
	runtime_semacquire(&w.sema)
}

func (s *FairSemaphore) TryAcquire(n int64) bool {
	if n <= 0 {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.head != nil || s.permits < n {
		return false
	}
	s.permits -= n
	return true
}

func (s *FairSemaphore) Release(n int64) {
	if n <= 0 {
		return
	}
	s.mu.Lock()
	s.permits += n
	for s.head != nil && s.permits >= s.head.n {
		w := s.head
		s.permits -= w.n
		s.head = w.next
		if s.head == nil {
			s.tail = nil
		}
		runtime_semrelease(&w.sema, false, 0)
	}
	s.mu.Unlock()
}
