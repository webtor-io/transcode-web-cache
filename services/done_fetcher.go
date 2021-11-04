package services

import (
	"context"
	"sync"
	"time"
)

type DoneFetcher struct {
	st     *S3Storage
	mux    sync.Mutex
	err    error
	res    bool
	inited bool
	ctx    context.Context
	key    string
	t      *time.Time
}

func NewDoneFetcher(ctx context.Context, st *S3Storage, key string) *DoneFetcher {
	return &DoneFetcher{
		st:  st,
		ctx: ctx,
		key: key,
	}
}

func (s *DoneFetcher) Fetch() (bool, *time.Time, error) {
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.inited {
		return s.res, s.t, s.err
	}
	s.res, s.t, s.err = s.fetch()
	s.inited = true
	return s.res, s.t, s.err
}

func (s *DoneFetcher) fetch() (res bool, t *time.Time, err error) {
	res, t, err = s.st.CheckDoneMarker(s.ctx, s.key)
	return
}
