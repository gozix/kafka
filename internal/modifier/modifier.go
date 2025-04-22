package modifier

import (
	"strings"

	"github.com/gobwas/glob"
	"github.com/gozix/di"
)

// Modifier is custom di Modifier
type Modifier struct {
	exclude []glob.Glob
	include []glob.Glob
	names   []string
	tag     string
}

const ArgNameKey = "name"

func NewModifier(tag string, patterns []string) (_ *Modifier, err error) {
	var m = &Modifier{
		exclude: make([]glob.Glob, 0, len(patterns)),
		include: make([]glob.Glob, 0, len(patterns)),
		names:   make([]string, 0, 8),
		tag:     tag,
	}

	// compile patterns, with negative support
	for _, pattern := range patterns {
		var index = 0
		if strings.HasPrefix(pattern, "!") {
			index = 1
		}

		var g glob.Glob
		if g, err = glob.Compile(pattern[index:]); err != nil {
			return nil, err
		}

		if index == 0 {
			m.include = append(m.include, g)
			continue
		}

		m.exclude = append(m.exclude, g)
	}

	// all included by default
	if len(m.include) == 0 {
		m.include = append(m.include, glob.MustCompile("*"))
	}

	return m, nil
}

func (m *Modifier) Modifier() di.Modifier {
	return di.Filter(func(def di.Definition) bool {
		// extract name
		var name string
		for _, tag := range def.Tags() {
			if tag.Name == m.tag {
				for _, arg := range tag.Args {
					if arg.Key == ArgNameKey {
						name = arg.Value
					}

					break
				}

				break
			}
		}

		if len(name) == 0 {
			return false
		}

		// filter
		for _, pattern := range m.exclude {
			if pattern.Match(name) {
				return false
			}
		}

		for _, pattern := range m.include {
			if pattern.Match(name) {
				m.names = append(m.names, name)
				return true
			}
		}

		return false
	})
}

func (m *Modifier) Name(index int) string {
	if 0 > index || index >= len(m.names) {
		return "unknown"
	}

	return m.names[index]
}
