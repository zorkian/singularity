/* singularity - safedoozer.go

   This package gives a little bit of convenience to using Doozer. We want to
   make sure that we die in most cases where we fail to talk to Doozer, so
   someone can use us to do that without all of the boilerplate.

*/

package safedoozer

import (
	"github.com/ha/doozer"
	logging "github.com/fluffle/golog/logging"
)

type Conn struct{
	Address string
	doozer.Conn
}

var log logging.Logger

func SetLogger(mlog logging.Logger) {
	log = mlog
}

func Dial(addr string) *Conn {
	dzr, err := doozer.Dial(addr)
	if err != nil {
		log.Fatal("failed to connect to %s: %s", addr, err)
	}
	return &Conn{Conn: *dzr, Address: addr}
}

func (dzr *Conn) Stat(file string, rev *int64) int64 {
	_, lrev, err := dzr.Conn.Stat(file, rev)
	if err != nil {
		log.Fatal("failed to stat file %s: %s", file, err)
	}
	return lrev
}

func (dzr *Conn) Set(file string, oldRev int64, body string) int64 {
	newRev, err := dzr.Conn.Set(file, oldRev, []byte(body))
	if err != nil {
		log.Fatal("failed to set %s: %s", file, err)
	}
	return newRev
}

func (dzr *Conn) GetLatest(file string) string {
	res, _, err := dzr.Conn.Get(file, nil)
	if err != nil {
		log.Fatal("failed to get %s: %s", file, err)
	}
	return string(res)
}

func (dzr *Conn) GetdirLatest(file string) []string {
	rev, err := dzr.Conn.Rev()
	if err != nil {
		log.Fatal("failed to get rev: %s", err)
	}

	dirs, err := dzr.Conn.Getdir(file, rev, 0, -1)
	if err != nil {
		log.Fatal("failed to getdir: %s", err)
	}
	return dirs
}
