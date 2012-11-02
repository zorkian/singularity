/* singularity - config.go

   Responsible for reading through our configuration files and parsing them
   into state-of-the-world objects.

*/

package main

import (
	"flag"
	"io/ioutil"
	yaml "launchpad.net/goyaml"
	"os"
	"path"
	"strconv"
	"strings"
)

var cfgVersion int64 = 0
var cfgFile *string

func init() {
	cfgFile = flag.String("config-dir", "", "directory for config files")
}

func initializeConfig() {
	if *cfgFile == "" {
		log.Info("no configuration directory specified, skipping config")
		return
	}

	*cfgFile = path.Clean(*cfgFile)
	log.Info("loading configuration directory: %s", *cfgFile)

	stat, err := os.Stat(*cfgFile)
	if err != nil {
		log.Fatal("failed to stat config dir: %s", err)
	}
	if !stat.IsDir() {
		log.Fatal("configuration directory is not a directory")
	}

	reloadConfig()
}

func reloadConfig() {
	cfgVersion++
	parseConfigFiles(*cfgFile)
}

func parseConfigFiles(fn string) {
	if strings.HasPrefix(fn, ".") {
		log.Debug("skipping dot-file: %s", fn)
		return
	}

	dir, err := os.Open(fn)
	if err != nil {
		log.Fatal("failed to open config dir: %s", err)
	}

	files, err := dir.Readdir(0)
	if err != nil {
		log.Fatal("failed in Readdir: %s", err)
	}

	for _, info := range files {
		if strings.HasPrefix(info.Name(), ".") {
			log.Debug("skipping dot-file: %s", info.Name())
			continue
		}

		newfn := path.Join(fn, info.Name())
		if info.IsDir() {
			parseConfigFiles(newfn)
		} else {
			if strings.HasSuffix(newfn, ".yaml") {
				parseConfigFile(newfn)
			} else {
				log.Debug("skipping non-YAML file: %s", info.Name())
			}
		}
	}
}

func parseConfigFile(fn string) {
	file, err := os.Open(fn)
	if err != nil {
		log.Error("failed to open %s: %s", fn, err)
		return
	}

	b, err := ioutil.ReadAll(file)
	if err != nil {
		log.Error("failed reading from %s: %s", fn, err)
		return
	}

	newstanzas := make(map[string]*sngConfig)
	err = yaml.Unmarshal(b, &newstanzas)
	if err != nil {
		log.Error("file is invalid %s: %s", fn, err)
	}

	// Validate configuration stanzas. This ensures that something is not
	// terribly broken with a config.
	for k, v := range newstanzas {
		v.Version = cfgVersion // Force to current version.
		v.Name = k
		v.Running = false
		mergeStanza(v)
	}
	synchronizeStanzas()
}

func (t *sngTime) SetYAML(tag string, value interface{}) bool {
	// If there is no suffix, it will be an int. This means we should just
	// use it as is with no conversions.
	switch value.(type) {
	case int:
		*t = sngTime(value.(int))
		return true
	}

	// This is a string that may or may not have a suffix like s, m, h, w.
	valstr := value.(string)
	factor := uint64(1)
	if len(valstr) >= 2 {
		switch valstr[len(valstr)-1] {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		case 's', 'S':
		case 'm', 'M':
			factor = 60
		case 'h', 'H':
			factor = 3600
		case 'd', 'D':
			factor = 86400
		case 'w', 'W':
			factor = 86400 * 7
		default:
			log.Error("Time string invalid: %s", valstr)
			return false
		}
		valstr = valstr[0 : len(valstr)-1]
	}

	// Now try to parse the remaining string and set it.
	val, err := strconv.ParseUint(valstr, 10, 64)
	if err != nil {
		log.Error("Time string '%s' invalid: %s", valstr, err)
		return false
	}
	*t = sngTime(val * factor)
	return true
}

func (t *sngBytes) SetYAML(tag string, value interface{}) bool {
	// If there is no suffix, it will be an int. This means we should just
	// use it as is with no conversions.
	switch value.(type) {
	case int:
		*t = sngBytes(value.(int))
		return true
	}

	// The user might have a 'b' suffix like 'mb', if so, let's trim that off
	// so we don't have to deal with it.
	valstr := value.(string)
	factor := uint64(1)
	if len(valstr) >= 3 {
		c := valstr[len(valstr)-2]
		if !(c >= '0' && c <= '9') {
			valstr = valstr[0 : len(valstr)-1]
		}
	}

	// This is a string that may or may not have a suffix like b, k, m, g, t.
	if len(valstr) >= 2 {
		switch valstr[len(valstr)-1] {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		case 'b', 'B':
		case 'k', 'K':
			factor = 1024
		case 'm', 'M':
			factor = 1024 * 1024
		case 'g', 'G':
			factor = 1024 * 1024 * 1024
		case 't', 'T':
			factor = 1024 * 1024 * 1024 * 1024
		default:
			log.Error("Byte string invalid: %s", valstr)
			return false
		}
		valstr = valstr[0 : len(valstr)-1]
	}

	// Now try to parse the remaining string and set it.
	val, err := strconv.ParseUint(valstr, 10, 64)
	if err != nil {
		log.Error("Byte string '%s' invalid: %s", valstr, err)
		return false
	}
	*t = sngBytes(val * factor)
	return true
}

// setDefault takes two pointers (dest, template) and a default value
// and then applys defaults logic. If p1 is nil, it may be replaced with
// the value from p2 if not nil, else, default.
//
// BUG(mark): Update this to use reflect.
//
func setDefault(dest, tmpl, defval interface{}) {
	// We handle strings and bools right now.
	switch dest.(type) {
	case **bool:
		setDefaultBool(dest.(**bool), tmpl.(**bool), defval)
	case **string:
		setDefaultString(dest.(**string), tmpl.(**string), defval)
	case **int64:
		setDefaultInt64(dest.(**int64), tmpl.(**int64), defval)
	case **sngBytes:
		setDefaultBytes(dest.(**sngBytes), tmpl.(**sngBytes), defval)
	case **sngTime:
		setDefaultTime(dest.(**sngTime), tmpl.(**sngTime), defval)
	default:
		log.Fatal("Attempted setDefault on unknown type: %T", dest)
	}
}

func setDefaultBool(dest, tmpl **bool, defval interface{}) {
	if *dest != nil {
		return
	}
	if *tmpl == nil {
		if defval == nil {
			*dest = nil
		} else {
			local := defval.(bool)
			*dest = &local
		}
	} else {
		local := **tmpl
		*dest = &local
	}
}

func setDefaultString(dest, tmpl **string, defval interface{}) {
	if *dest != nil {
		return
	}
	if *tmpl == nil {
		if defval == nil {
			*dest = nil
		} else {
			local := defval.(string)
			*dest = &local
		}
	} else {
		local := **tmpl
		*dest = &local
	}
}

func setDefaultBytes(dest, tmpl **sngBytes, defval interface{}) {
	if *dest != nil {
		return
	}
	if *tmpl == nil {
		if defval == nil {
			*dest = nil
		} else {
			local := defval.(sngBytes)
			*dest = &local
		}
	} else {
		local := **tmpl
		*dest = &local
	}
}

func setDefaultTime(dest, tmpl **sngTime, defval interface{}) {
	if *dest != nil {
		return
	}
	if *tmpl == nil {
		if defval == nil {
			*dest = nil
		} else {
			local := defval.(sngTime)
			*dest = &local
		}
	} else {
		local := **tmpl
		*dest = &local
	}
}

func setDefaultInt64(dest, tmpl **int64, defval interface{}) {
	if *dest != nil {
		return
	}
	if *tmpl == nil {
		if defval == nil {
			*dest = nil
		} else {
			local := defval.(int64)
			*dest = &local
		}
	} else {
		local := **tmpl
		*dest = &local
	}
}
