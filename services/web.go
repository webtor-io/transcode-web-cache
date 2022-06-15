package services

import (
	"crypto/sha1"
	"fmt"
	"net"
	"net/http"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

const (
	webHostFlag    = "host"
	webPortFlag    = "port"
	keyPrefixFlag  = "key-prefix"
	originPathFlag = "origin-path"
	infoHashFlag   = "info-hash"
	playerFlag     = "player"
)

type Web struct {
	host string
	port int
	kp   string
	op   string
	ih   string
	c    *LookaheadCache
	tp   *TouchPool
	dp   *DonePool
	ln   net.Listener
	pl   bool
}

func NewWeb(c *cli.Context, ca *LookaheadCache, tp *TouchPool, dp *DonePool) *Web {
	return &Web{
		host: c.String(webHostFlag),
		port: c.Int(webPortFlag),
		kp:   c.String(keyPrefixFlag),
		op:   c.String(originPathFlag),
		ih:   c.String(infoHashFlag),
		pl:   c.Bool(playerFlag),
		c:    ca,
		tp:   tp,
		dp:   dp,
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
	c.Flags = append(c.Flags, cli.StringFlag{
		Name:  keyPrefixFlag,
		Usage: "key prefix",
		Value: "",
	})
	c.Flags = append(c.Flags, cli.StringFlag{
		Name:  originPathFlag,
		Usage: "origin path",
		Value: "",
	})
	c.Flags = append(c.Flags, cli.StringFlag{
		Name:  infoHashFlag,
		Usage: "info hash",
		Value: "",
	})
	c.Flags = append(c.Flags, cli.BoolFlag{
		Name:  playerFlag,
		Usage: "player",
	})
}

func (s *Web) getKeyPrefix(r *http.Request) string {
	if s.kp != "" {
		return s.kp
	}
	if r.URL.Query().Get("prefix") != "" {
		return r.URL.Query().Get("prefix")
	}
	if r.Header.Get("X-Key-Prefix") != "" {
		return r.Header.Get("X-Key-Prefix")
	}
	return "transcoder"
}

func (s *Web) getOriginPath(r *http.Request) string {
	if s.op != "" {
		return s.op
	}
	if r.URL.Query().Get("path") != "" {
		return r.URL.Query().Get("path")
	}
	if r.Header.Get("X-Origin-Path") != "" {
		return r.Header.Get("X-Origin-Path")
	}
	return ""
}

func (s *Web) getInfoHash(r *http.Request) string {
	if s.ih != "" {
		return s.ih
	}
	if r.URL.Query().Get("hash") != "" {
		return r.URL.Query().Get("hash")
	}
	if r.Header.Get("X-Info-Hash") != "" {
		return r.Header.Get("X-Info-Hash")
	}
	return ""
}

func (s *Web) getKey(r *http.Request) string {
	key := fmt.Sprintf("%x", sha1.Sum([]byte(s.getKeyPrefix(r)+s.getInfoHash(r)+s.getOriginPath(r))))
	return key
}

func (s *Web) Serve() error {
	addr := fmt.Sprintf("%s:%d", s.host, s.port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.Wrap(err, "failed to web listen to tcp connection")
	}
	mux := http.NewServeMux()
	if s.pl {
		log.Info(fmt.Sprintf("Player available at http://%v/player/", addr))
		mux.Handle("/player/", http.StripPrefix("/player/", http.FileServer(http.Dir("./player"))))
	}
	mux.HandleFunc("/done", func(w http.ResponseWriter, r *http.Request) {
		done, _, err := s.dp.Done(s.getKey(r))
		w.Header().Set("X-Cache-Key", s.getKey(r))
		if err != nil {
			log.WithError(err).Error("failed to check done marker")
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
		key := s.getKey(r)
		w.Header().Set("X-Cache-Key", key)
		d, t, err := s.dp.Done(s.getKey(r))
		if err != nil {
			log.WithError(err).Error("failed to check done marker")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if !d {
			log.Error("transcoding not done yet")
			w.WriteHeader(http.StatusNotFound)
			return
		}
		c, err := s.c.Get(key, r.URL.Path)
		if err != nil {
			log.WithError(err).Error("failed to serve content")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if c == nil {
			log.Warnf("content not found path=%v hash=%v key=%v", s.getOriginPath(r), s.getInfoHash(r), key)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		defer c.Close()
		http.ServeContent(w, r, "", *t, c)
		go func() {
			err := s.tp.Touch(key)
			if err != nil {
				log.WithError(err).Error("failed to touch")
			}
		}()
	})
	log.Infof("serving Web at %v", addr)
	return http.Serve(ln, allowCORSHandler(enrichPlaylistHandler(mux)))
}

func (s *Web) Close() {
	if s.ln != nil {
		s.ln.Close()
	}
}
