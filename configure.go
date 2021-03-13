package main

import (
	"net"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	cs "github.com/webtor-io/common-services"
	s "github.com/webtor-io/transcode-web-cache/services"
)

func configure(app *cli.App) {
	app.Flags = []cli.Flag{}
	cs.RegisterProbeFlags(app)
	cs.RegisterS3ClientFlags(app)
	s.RegisterS3StorageFlags(app)
	s.RegisterWebFlags(app)
	app.Action = run
}

func run(c *cli.Context) error {
	// Setting HTTP Client
	myTransport := &http.Transport{
		MaxIdleConns:        500,
		MaxIdleConnsPerHost: 50,
		Dial: (&net.Dialer{
			Timeout: 5 * time.Minute,
		}).Dial,
	}
	cl := &http.Client{
		Timeout:   5 * time.Minute,
		Transport: myTransport,
	}

	// Setting S3 Client
	s3cl := cs.NewS3Client(c, cl)

	// Setting S3 Storage
	s3st := s.NewS3Storage(c, s3cl)

	// Setting TouchPool
	tp := s.NewTouchPool(s3st)

	// Setting DonePool
	dp := s.NewDonePool(s3st)

	// Setting ProbeService
	probe := cs.NewProbe(c)
	defer probe.Close()

	// Setting WebService
	web := s.NewWeb(c, s3st, tp, dp)
	defer web.Close()

	// Setting ServeService
	serve := cs.NewServe(probe, web)

	// And SERVE!
	err := serve.Serve()
	if err != nil {
		log.WithError(err).Error("Got server error")
	}
	return nil
}
