package services

type Queue struct {
	ch     chan func()
	c      int
	closed bool
	inited bool
}

func NewQueue(c int) *Queue {
	return &Queue{
		ch: make(chan func()),
		c:  c,
	}
}

func (s *Queue) Close() {
	if s.closed {
		return
	}
	s.closed = true
	close(s.ch)
}

func (s *Queue) Push(f func()) {
	if !s.inited {
		s.inited = true
		for i := 0; i < s.c; i++ {
			go func() {
				for i := range s.ch {
					i()
				}
			}()
		}
	}
	s.ch <- f
}
