/* singularity - stanzas.go

   What we call 'stanzas' are objects/configurations that we load up and
   keep track of. These are things that might be templates, triggers, plugins,
   or jobs.

   This package keeps together all of the code related to stanza management
   and validation.

*/

package main

import (
   "errors"
   "strings"
   "time"
)

type sngTime int64
type sngBytes int64

type sngParams struct {
	JobCount *int64 "job.count"
	LogsKeep *sngBytes "logs.keep"
	Cooldown *sngTime
}

type sngConfig struct {
	Name string
	Running bool
	IsTemplate bool "is_template"
	IsPlugin *bool "is_plugin"
	Template *string
	Trigger *string
	Binary *string
	Scope *string
	Prereqs []string
	Params sngParams
	Version int64 // cfgVersion last touched.
}

var stanzas map[string]*sngConfig = make(map[string]*sngConfig)

// isValidStanza returns a boolean indicating whether or not a given stanza
// looks valid. This also sets defaults if they're not set.
func isValidStanza(cfg *sngConfig) (err error) {
	// Scope must be one of 'local', 'global'
	if cfg.Scope == nil {
		return errors.New("undefined scope")
	}
	*cfg.Scope = strings.ToLower(*cfg.Scope)
	if *cfg.Scope != "local" && *cfg.Scope != "global" {
		return errors.New("scope must be one of 'local', 'global'")
	}

	// Templates must not have templates.
	if cfg.IsTemplate && cfg.Template != nil {
		return errors.New("templates can't be nested")
	}

	// Everything looks good.
	return nil
}

// mergeStanza takes a stanza and attempts to merge it in to the state of the
// world. This might cause errors if something bad has happened.
func mergeStanza(cfg *sngConfig) {
	old := stanzas[cfg.Name]
	if old != nil && old.Version == cfg.Version {
		log.Error("Two configs named %s!", cfg.Name)
	}

	// BUG(mark): When old is already running, we can't stop the babysitter
	// because it's off in a goroutine. What do we do?

	stanzas[cfg.Name] = cfg
}

// stanzaExists returns a boolean indicating whether or not a given stanza
// is presently defined.
func stanzaExists(name string) bool {
	_, ok := stanzas[name]
	return ok
}

// synchronizeStanzas iterates over the existing configuration stanzas and
// updates them as necessary, does sanity checks, etc.
func synchronizeStanzas() {
	// Do a pass to delete anything that is an old version. This is important
	// to happen first so we don't depend on these later.
	for name, cfg := range stanzas {
		if cfg.Version < cfgVersion {
			log.Debug("Config %s no longer exists, forgetting.", name)
			delete(stanzas, name)
		}
	}

	// Now everything is current at least, let's go through and vivify things
	// from templates.
	for _, cfg := range stanzas {
		cfg.applyTemplate()
		if err := isValidStanza(cfg); err != nil {
			log.Error("Config %s invalid: %s", cfg.Name, err)
			continue
		}
	}

	// Finally, remove templates. We don't care about them anymore, at least
	// not until the next time the configs get loaded.
	for name, cfg := range stanzas {
		if cfg.IsTemplate {
			delete(stanzas, name)
		}
	}	

	log.Debug("Configuration stanzas synchronized.")
}

// applyTemplate triggers a stanza to actually apply a template.
func (cfg *sngConfig) applyTemplate() {
	// Do nothing if we don't have a template to apply, or it doesn't exist.
	if cfg.IsTemplate {
		return
	}
	var tmpl *sngConfig
	if cfg.Template == nil {
		tmpl = &sngConfig{}
	} else {
		tmpl = stanzas[*cfg.Template]
		if tmpl == nil {
			log.Error("Config %s wants template %s, but the template does not exist.",
				cfg.Name, cfg.Template)
			tmpl = &sngConfig{}
		} else {
			log.Debug("Applying template %s to %s...", tmpl.Name, cfg.Name)
		}
	}

	// We need to copy over things from our template, but only if they aren't
	// set locally. setDefault is a handy utility method to do that.
	setDefault(&cfg.IsPlugin, &tmpl.IsPlugin, false)
	setDefault(&cfg.Trigger, &tmpl.Trigger, nil)
	setDefault(&cfg.Binary, &tmpl.Binary, nil)
	setDefault(&cfg.Scope, &tmpl.Scope, "local")

	// Parameter defaults.	
	setDefault(&cfg.Params.JobCount, &tmpl.Params.JobCount, int64(0))
	setDefault(&cfg.Params.LogsKeep, &tmpl.Params.LogsKeep, sngBytes(0))
	setDefault(&cfg.Params.Cooldown, &tmpl.Params.Cooldown, sngTime(0))
}

// maintainStanzas launches goroutines for our jobs that have not been run.
func maintainStanzas() {
	babysitter := func(cfg *sngConfig) {
		defer func() {
			if err := recover(); err != nil {
				log.Error("%s worker panicked, dropping: %s", cfg.Name, err)
			}
		}()

		// Record start time, run, check end time. If less than cooldown or a
		// more sane amount of time, sleep before running again.
		log.Debug("%s babysitter starting", cfg.Name)
		curver := cfg.Version
		for {
			cfg.activate()

			time.Sleep(1 * time.Second)
			if cfgVersion > curver {
				log.Debug("%s worker is old, exiting", cfg.Name)
				return
			}
		}
	}

	for _, cfg := range stanzas {
		if cfg.Running {
			continue
		}
		cfg.Running = true
		go babysitter(cfg)
	}
}

// activate runs this particular config, if it meets the prerequisites
func (cfg *sngConfig) activate() {

}