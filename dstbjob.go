package dstbjob

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"github.com/robfig/cron/v3"
)

const dstbLockKeyPrefix = "dstblocker"

const defaultRetries = 3

// DSTBJob distributed cronjob service
type DSTBJob struct {
	systemName string
	node       string
	locker     *redsync.Redsync
	cronjob    *cron.Cron
	log        Logger
}

// NewDSTBJob create DSTBJob
func NewDSTBJob(conf Config, r *redis.Client, opts ...interface{}) (*DSTBJob, error) {
	var l Logger
	if len(opts) > 0 {
		for _, opt := range opts {
			switch vv := opt.(type) {
			case Logger:
				l = vv
			}
		}
	}
	if l == nil {
		l = &defaultLogger{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := r.Ping(ctx).Result(); err != nil {
		return nil, err
	}
	red := redsync.New(goredis.NewPool(r))

	return &DSTBJob{
		systemName: conf.SystemName,
		node:       conf.Node,
		locker:     red,
		cronjob:    cron.New(),
		log:        l,
	}, nil
}

// AddFunc add cron function
func (s *DSTBJob) AddFunc(name, spec string, cmd func()) (cron.EntryID, error) {
	sched, err := cron.ParseStandard(spec)
	if err != nil {
		return 0, err
	}

	next := sched.Next(time.Now())
	ttl := sched.Next(next).Sub(next) - time.Millisecond*50

	// logger prefix
	logpref := fmt.Sprintf("sys:%s node: %s job: %s", s.systemName, s.node, name)
	s.log.Debugf("%s ttl: %v\n", logpref, ttl)

	// default retry delay is from 50ms to 250ms, retry 3 times won't exceed 1 second(minimum interval for cronjob)
	mutex := s.locker.NewMutex(fmt.Sprintf("%s:%s:%s", dstbLockKeyPrefix, s.systemName, name), redsync.WithExpiry(ttl), redsync.WithTries(defaultRetries))

	return s.cronjob.AddFunc(spec, func() {
		start := time.Now()
		s.log.Debugf("%s start\n", logpref)
		err := mutex.Lock()
		if err != nil {
			if err == redsync.ErrFailed {
				s.log.Debugf("%v\n", err)
			} else {
				s.log.Errorf("%s lock err: %v\n", logpref, err)
			}
			return
		}
		s.log.Debugf("%s get lock\n", logpref)

		// execute cronjob
		cmd()

		// If the execution time cost is too short, another node may run again in the same period.
		// cost time greater than one second is safe for retries(3 times with 50ms to 250ms delay)
		if time.Since(start) >= time.Second {
			s.log.Debugf("%s unlock\n", logpref)
			mutex.Unlock()
		}
		s.log.Debugf("%s end\n", logpref)
	})
}

// Start start dstb cronjob
func (s *DSTBJob) Start() {
	s.cronjob.Start()
}

// Stop stop dstb cronjob
func (s *DSTBJob) Stop() context.Context {
	return s.cronjob.Stop()
}

// Logger logger interface use by dstbjob
type Logger interface {
	Debugf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

type defaultLogger struct {
}

// Debugf default debugf do nothing
func (l *defaultLogger) Debugf(format string, args ...interface{}) {
	return
}

// Errorf default errorf
func (l *defaultLogger) Errorf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}
