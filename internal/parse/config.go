package parse

import (
	"strings"
)

func ParseAuthParams(s string) (authParams map[string]string) {
	params := strings.Split(s, ",")
	authParams = make(map[string]string, len(params))
	for _, s := range params {
		keyAndValue := strings.Split(s, ":")
		if len(keyAndValue) > 1 {
			authParams[keyAndValue[0]] = keyAndValue[1]
		}
	}
	return
}
