package services

import (
	"context"
	"sync"
	"time"
)

const (
	doneTTL = 600
)

type DonePool struct {
	sm     sync.Map
	st     *S3Storage
	expire time.Duration
}

func NewDonePool(st *S3Storage) *DonePool {
	return &DonePool{
		expire: time.Duration(doneTTL) * time.Second,
		st:     st,
	}
}

func (s *DonePool) Done(key string) (bool, *time.Time, error) {
	df, loaded := s.sm.LoadOrStore(key, NewDoneFetcher(context.Background(), s.st, key))
	if !loaded {
		go func() {
			<-time.After(s.expire)
			s.sm.Delete(key)
		}()
	}
	return df.(*DoneFetcher).Fetch()
}
