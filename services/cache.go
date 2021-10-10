package services

import (
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/webtor-io/lazymap"
)

const (
	preloadCachePath = "cache"
)

type Cache struct {
	lazymap.LazyMap
	s3st *S3Storage
	path string
}

func NewCache(s3st *S3Storage) *Cache {
	return &Cache{
		s3st: s3st,
		path: preloadCachePath,
		LazyMap: lazymap.New(&lazymap.Config{
			Concurrency: 10,
			Expire:      60 * time.Second,
			ErrorExpire: 30 * time.Second,
			Capacity:    1000,
		}),
	}
}

func (s *Cache) makeKey(key string, path string) string {
	return fmt.Sprintf("%x", sha1.Sum([]byte(key+path)))
}

func (s *Cache) Get(key string, path string) (io.ReadCloser, error) {
	fPath := s.path + "/" + s.makeKey(key, path)
	err := s.Preload(key, path)
	if err != nil {
		if _, ok := err.(*NotFoundError); ok {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "Failed to preload key=%v path=%v", key, path)
	}
	f, err := os.Open(fPath)
	if err != nil {
		return nil, err
	}
	return f, nil
}

type NotFoundError struct {
	error
}

func (s *Cache) Preload(key string, path string) error {
	kk := s.makeKey(key, path)
	_, err := s.LazyMap.Get(kk, func() (interface{}, error) {
		p := s.path + "/" + kk
		tp := s.path + "/_" + kk
		if _, err := os.Stat(p); os.IsNotExist(err) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			c, err := s.s3st.GetContent(ctx, key, path)
			if err != nil {
				return nil, errors.Wrapf(err, "Failed to get S3 content key=%v path=%v", key, path)
			}
			if c == nil {
				return nil, &NotFoundError{}
			}
			f, err := os.Create(tp)
			if err != nil {
				return nil, errors.Wrapf(err, "Failed to create preload file path=%v", tp)
			}
			_, err = io.Copy(f, c)
			if err != nil {
				return nil, errors.Wrapf(err, "Failed to copy data path=%v", tp)
			}
			err = os.Rename(tp, p)
			if err != nil {
				return nil, errors.Wrapf(err, "Failed to rename file from=%v to=%v", tp, p)
			}
			return nil, nil
		} else {
			t := time.Now().Local()
			err := os.Chtimes(p, t, t)
			if err != nil {
				return nil, errors.Wrapf(err, "Failed to change preload file modification date path=%v", p)
			}
			log.Infof("Preload data already exists path=%v", p)
			return nil, nil
		}
	})
	return err
}
