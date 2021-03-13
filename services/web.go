package services

import (
	"crypto/sha1"
	"fmt"
	"io"
	"net"
	"net/http"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

const (
	webHostFlag = "host"
	webPortFlag = "port"
)

type Web struct {
	host string
	port int
	src  string
	s3st *S3Storage
	tp   *TouchPool
	ln   net.Listener
}

func NewWeb(c *cli.Context, s3st *S3Storage, tp *TouchPool) *Web {
	return &Web{
		host: c.String(webHostFlag),
		port: c.Int(webPortFlag),
		s3st: s3st,
		tp:   tp,
	}
}

func RegisterWebFlags(c *cli.App) {
	c.Flags = append(c.Flags, cli.StringFlag{
		Name:  webHostFlag,
		Usage: "listening host",
		Value: "",
	})
	c.Flags = append(c.Flags, cli.IntFlag{
		Name:  webPortFlag,
		Usage: "http listening port",
		Value: 8080,
	})
}

func getKey(r *http.Request) string {
	path, infohash := r.Header.Get("X-Origin-Path"), r.Header.Get("X-Info-Hash")
	key := fmt.Sprintf("%x", sha1.Sum([]byte("transcoder"+infohash+path)))
	return key
}

func (s *Web) Serve() error {
	addr := fmt.Sprintf("%s:%d", s.host, s.port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.Wrap(err, "Failed to web listen to tcp connection")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/done", func(w http.ResponseWriter, r *http.Request) {
		done, err := s.s3st.CheckDoneMarker(r.Context(), getKey(r))
		if err != nil {
			log.WithError(err).Error("Failed to check done marker")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if !done {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		key := getKey(r)
		c, err := s.s3st.GetContent(r.Context(), key, r.URL.Path)
		if err != nil {
			log.WithError(err).Error("Failed to serve content")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if c == nil {
			log.Warnf("Content not found path=%v hash=%v key=%v", r.Header.Get("X-Info-Hash"), r.Header.Get("X-Origin-Path"), key)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		defer c.Close()
		_, err = io.Copy(w, c)
		if err != nil {
			log.WithError(err).Error("Failed to read content")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		go func() {
			err := s.tp.Touch(key)
			if err != nil {
				log.WithError(err).Error("Failed to touch")
			}
		}()
	})
	log.Infof("Serving Web at %v", addr)
	return http.Serve(ln, allowCORSHandler(mux))
}

func (s *Web) Close() {
	if s.ln != nil {
		s.ln.Close()
	}
}
