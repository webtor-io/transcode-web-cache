package services

import (
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/webtor-io/lazymap"
)

const (
	lookaheadNum int = 10
)

type Fragment struct {
	num    int
	prefix string
	suffix string
}

func NewFragment(str string) *Fragment {
	parts := strings.Split(str, ".")
	if len(parts) != 2 {
		return nil
	}
	parts2 := strings.Split(parts[0], "-")
	if len(parts) < 2 {
		return nil
	}
	last := parts2[len(parts2)-1]
	num, err := strconv.Atoi(last)
	if err != nil {
		return nil
	}
	return &Fragment{
		num:    num,
		prefix: strings.Join(parts2[0:len(parts2)-1], "-") + "-",
		suffix: "." + parts[1],
	}
}

func (s *Fragment) Inc(i int) *Fragment {
	return &Fragment{
		num:    s.num + i,
		prefix: s.prefix,
		suffix: s.suffix,
	}
}

func (s *Fragment) String() string {
	return s.prefix + strconv.Itoa(s.num) + s.suffix
}

type LookaheadCache struct {
	lazymap.LazyMap
	c *Cache
	n int
}

func NewLookaheadCache(c *Cache) *LookaheadCache {
	return &LookaheadCache{
		c: c,
		n: lookaheadNum,
		LazyMap: lazymap.New(&lazymap.Config{
			Concurrency: 100,
			Expire:      60 * time.Second,
			ErrorExpire: 30 * time.Second,
			Capacity:    1000,
		}),
	}
}

func (s *LookaheadCache) Get(key string, path string) (io.ReadCloser, error) {
	go s.Preload(key, path)
	return s.c.Get(key, path)
}

func (s *LookaheadCache) Preload(key string, path string) {
	f := NewFragment(path)
	if f == nil {
		return
	}
	kk := key + f.prefix + f.suffix
	v, _ := s.LazyMap.Get(kk, func() (interface{}, error) {
		q := NewQueue(3)
		return q, nil
	})
	q := v.(*Queue)
	for i := 1; i < lookaheadNum+1; i++ {
		go func(i int) {
			q.Push(func() {
				s.c.Preload(key, f.Inc(i).String())
			})
		}(i)
	}
}
