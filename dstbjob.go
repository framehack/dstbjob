package dstbjob

import (
	"context"
	"time"

	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v8"
	"github.com/robfig/cron/v3"
)

const dstbLockKeyPrefix = "dstblocker:"

// DSTBJob distributed cronjob service
type DSTBJob struct {
	systemName string
	node       string
	locker     *redislock.Client
	cronjob    *cron.Cron
}

// NewDSTBJob create DSTBJob
func NewDSTBJob(system string, node string, r *redis.Client) (*DSTBJob, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := r.Ping(ctx).Result(); err != nil {
		return nil, err
	}
	return &DSTBJob{
		systemName: system,
		node:       node,
		locker:     redislock.New(r),
		cronjob:    cron.New(),
	}, nil
}

// AddFunc add cron function
func (s *DSTBJob) AddFunc(spec string, cmd func()) (cron.EntryID, error) {
	sched, err := cron.ParseStandard(spec)
	if err != nil {
		return 0, err
	}
	now := time.Now()
	ttl := sched.Next(now).Sub(now)
	return s.cronjob.AddFunc(spec, func() {
		ctx := context.TODO()
		_, err := s.locker.Obtain(ctx, dstbLockKeyPrefix+s.systemName, ttl, nil)
		if err == redislock.ErrNotObtained {
			return
		} else if err != nil {
			return
		}
		cmd()
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
