package dstbjob

import (
	"fmt"
	"testing"

	"github.com/go-redis/redis/v8"
)

type logger struct {
	t *testing.T
}

func (l *logger) Debugf(f string, args ...interface{}) {
	l.t.Logf(f, args...)
}

func (l *logger) Errorf(f string, args ...interface{}) {
	l.t.Logf(f, args...)
}

func TestJob(t *testing.T) {
	r := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	count := 0
	l := &logger{t}
	stop := make(chan struct{}, 1)
	stopat := 5
	interval := 1
	jobname := "everysecond"
	errs := make(chan error, 1)
	j, err := NewDSTBJob(Config{
		SystemName: "test",
		Node:       "n1",
	}, r, l)
	if err != nil {
		t.Fatal(err)
	}

	_, err = j.AddFunc(jobname, fmt.Sprintf("@every %ds", interval), func() {
		count++
		t.Logf("inc1: %d\n", count)
		if count >= stopat {
			stop <- struct{}{}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = j.AddFunc(jobname, fmt.Sprintf("@every %ds", interval), func() {
		count++
		t.Logf("inc2: %d\n", count)
		if count >= stopat {
			stop <- struct{}{}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		j2, err := NewDSTBJob(Config{
			SystemName: "test",
			Node:       "n2",
		}, r, l)
		if err != nil {
			errs <- err
		}
		_, err = j2.AddFunc(jobname, fmt.Sprintf("@every %ds", interval), func() {
			count++
			t.Logf("inc3: %d\n", count)
			if count >= stopat {
				stop <- struct{}{}
			}
		})
		if err != nil {
			errs <- err
		}
	}()
	j.Start()
	select {
	case <-stop:
		break
	case err = <-errs:
		t.Fatal(err)
	}
	if count != stopat {
		t.Errorf("error stop: %d, expect: %d", count, stopat)
	}
	j.Stop()
}
