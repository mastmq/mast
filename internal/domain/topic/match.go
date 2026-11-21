package topic

import "strings"

// Matches reports whether an MQTT topic filter selects a topic name, by the
// rules of MQTT 5 section 4.7: '+' matches exactly one level, an empty one
// included; '#' matches any number of levels, its parent too, so "a/#"
// selects "a"; and a filter whose first level is a wildcard never selects a
// name beginning with '$' [MQTT-4.7.2-1]. One quirk of mochi's is kept on
// purpose; see the "+/#" case below.
//
// Most matching in mast is done by NATS on encoded subjects or by mochi's
// topic index on tenant-mounted topics, and neither knows the '$' rule
// applies to the level after the tenant. This is the reference both are
// corrected against.
func Matches(filter, name string) bool {
	if name == "" {
		return false // not a topic name at all
	}

	if StartsWithWildcard(filter) && strings.HasPrefix(name, "$") {
		return false
	}

	return matchLevels(filter, name)
}

// matchLevels walks a filter and a name level by level.
func matchLevels(filter, name string) bool {
	for {
		level, rest, more := strings.Cut(filter, "/")
		if level == "#" {
			return true
		}

		nameLevel, nameRest, nameMore := strings.Cut(name, "/")
		if level != "+" && level != nameLevel {
			return false
		}

		if !more {
			return !nameMore
		}

		if !nameMore {
			// The name has run out; only a trailing "/#" still matches,
			// and, as in mochi, not when the level before it was matched
			// by '+'. The spec would have "+/#" select "a"; mochi's index
			// decides live delivery and does not, and every other path
			// has to give the device the answer that one does.
			return rest == "#" && level != "+"
		}

		filter, name = rest, nameRest
	}
}

// StartsWithWildcard reports whether a filter's first level is '+' or '#',
// the filters MQTT forbids from selecting a topic that begins with '$'.
func StartsWithWildcard(filter string) bool {
	return filter != "" && (filter[0] == '+' || filter[0] == '#')
}
