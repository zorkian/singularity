/* singularity - agent - trigger.go

   Logic for the trigger system.

*/

package main

import (
	"encoding/json"
	"strings"
	"sync"
)

// These are functions that are called when a trigger is fired. The return value
// is whether this trigger should stay alive. A false return will cause us to
// drop this trigger function and never call it again.
type triggerFunc func(event string, payload interface{}) bool

// The locking granularity is such that only one trigger can be executed per
// "set" at the same time. We could be executing triggers from different sets,
// but only ever one in a set is active.
type triggerSet struct {
	lock  sync.Mutex
	funcs []triggerFunc
}

// This lock is designed to held only for very short moments to make sure that
// we are synchronized when touching the map.
var triggerMap map[string]triggerSet = make(map[string]triggerSet)
var triggerLock sync.Mutex // protects triggerMap

// isValidJSON simply parses the JSON and returns the object if it's valid or
// nil if it wasn't valid.
func isValidJSON(obj string) interface{} {
	var out interface{}
	err := json.Unmarshal([]byte(obj), &out)
	if err != nil {
		log.Debug("invalid JSON: %s", err)
		return nil
	}
	return out
}

// doSendTrigger is called when a trigger needs to be sent out, possibly to
// remote agents.
func doSendTrigger(trigger, scope, payload string) bool {
	json_payload := isValidJSON(payload)
	if json_payload == nil {
		return false
	}

	trigger = strings.TrimSpace(trigger)
	if trigger == "" || strings.Index(trigger, " ") > -1 {
		return false
	}

	// Looks valid as far as we can tell. If this is a local trigger, we skip
	// the destination logic and just send it out to our local handler.
	switch scope {
	case "local":
		doTrigger(trigger, json_payload)
	case "regional":
		log.Error("regional triggers not implemented yet")
		return false
	case "global":
		log.Error("global triggers not implemented yet")
		return false
	default:
		return false
	}

	return true
}

// doTrigger takes a trigger and payload and processes them among the local
// stanzas that we have.
func doTrigger(trigger string, payload interface{}) {
	log.Debug("trigger [%s] fired", trigger)

	// Minimal lock zone, so we can't defer.
	triggerLock.Lock()
	tset, ok := triggerMap[trigger]
	if !ok {
		tset = triggerSet{}
		triggerMap[trigger] = tset
	}
	triggerLock.Unlock()

	// Slightly more contentious here since we might be updating this set.
	tset.lock.Lock()
	defer tset.lock.Unlock()

	// Now we have a tset, let's iterate and try to run some triggers.
	for idx, tfunc := range tset.funcs {
		log.Debug("trigger [%s] firing a runnable", trigger)
		if !tfunc(trigger, payload) {
			log.Debug("trigger [%s] func requested removal", trigger)
			tset.funcs[idx] = nil
		}
	}
}
