/* singularity - agent - facter.go

   For now, we use Facter to gain information about this node's configuration
   and other data.

*/

package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"time"
)

func getSelfInfo() (InfoMap, error) {
	myinfo := make(InfoMap)

	lhostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	myinfo["hostname"] = lhostname

	// For now, we are depending on facter to give us information about this
	// system. I think that I like that? We should also investigate if Chef has
	// a similar tool and if we can just use either.
	cmd := exec.Command("facter")
	output, err := cmd.Output()
	if err != nil {
		log.Fatal("failed to run facter: %s", err)
	}

	facts := strings.Split(string(output), "\n")
	for _, factline := range facts {
		fact := strings.SplitN(factline, " => ", 2)
		if len(fact) != 2 {
			continue
		}
		// _ is not allowed by Doozer (?!), but . is.
		myinfo[strings.Replace(fact[0], "_", ".", -1)] =
			strings.TrimSpace(fact[1])
	}
	return myinfo, nil
}

func maintainInfo(info *InfoMap) {
	// We want to keep our original hostname. If it ever changes, we need to
	// bail out. Right now we're locking on hostname, so that means somebody
	// else might start up with the new name...
	lhostname := (*info)["hostname"]
	path := "/s/node/" + lhostname

	for {
		newmyinfo, err := getSelfInfo()
		if err != nil {
			log.Fatal("failed updating info: %s", err)
		}

		// Hostname change check, as noted above.
		newhostname, ok := newmyinfo["hostname"]
		if newhostname != lhostname || newhostname != hostname || !ok {
			//			log.Fatal("hostname changed mid-flight!")
		}

		// We can get the current repository revision because we are asserting
		// that nobody else is updating these keys, and that we will only touch
		// each key at most once. That way we don't have to track further revs.
		rev := dzr.Rev()

		// Delete keys that have vanished from Old to New.
		for key, _ := range *info {
			keypath := fmt.Sprintf("%s/%s", path, key)
			_, ok := newmyinfo[key]
			if !ok {
				err := dzr.Del(keypath, rev)
				if err != nil {
					log.Fatal("failed to delete %s: %s", keypath, err)
				}
				delete(*info, key)
			}
		}

		// Now we can just copy over things from New to Old (so that the global
		// structure is fine) and update Doozer.
		for key, _ := range newmyinfo {
			keypath := fmt.Sprintf("%s/%s", path, key)
			_, err := dzr.Conn.Set(keypath, rev, []byte(newmyinfo[key]))
			if err != nil {
				log.Fatal("failed to set %s: %s", keypath, err)
			}
			(*info)[key] = newmyinfo[key]
		}

		// To prevent stampeding (if you restart all of your clients near the
		// same time), vary the sleep time from 150..450 seconds.
		time.Sleep(time.Duration(150+rand.Intn(300)) * time.Second)
	}
}
