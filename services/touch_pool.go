package services

import (
	"context"
	"sync"
	"time"
)

const (
	touchTTL = 60
)

type TouchPool struct {
	sm     sync.Map
	st     *S3Storage
	expire time.Duration
}

func NewTouchPool(st *S3Storage) *TouchPool {
	return &TouchPool{
		expire: time.Duration(touchTTL) * time.Second,
		st:     st,
	}
}

func (s *TouchPool) Touch(key string) error {
	_, loaded := s.sm.LoadOrStore(key, true)
	if !loaded {
		t := NewToucher(context.Background(), s.st, key)
		go func() {
			<-time.After(s.expire)
			s.sm.Delete(key)
		}()
		return t.Touch()
	}
	return nil
}
