package pgx

import (
	"errors"
	"strings"
)

func ParseDsn(s string) (map[string]string, error) {
	ss := strings.Split(s, " ")
	out := make(map[string]string)
	for _, s := range ss {
		kv := strings.Split(s, "=")
		if len(kv) != 2 {
			return nil, errors.New("Invalid dsn at:" + s)
		}
		out[kv[0]] = kv[1]
	}
	return out, nil
}
