package main

import (
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"unicode/utf8"
)

type consentLevel string

const (
	_           consentLevel = ""
	Functional               = "FUNCTIONAL"
	Performance              = "PERFORMANCE"
	Targeting                = "TARGETING"
)

type consentCodex struct {
	ConsentKeys     map[string]consentLevel `yaml:"consent_keys"`
	TrueValue       string                  `yaml:"true_value"`
	FieldDelim      string                  `yaml:"field_delim"`
	KeyDelim        string                  `yaml:"key_delim"`
	URLDecode       bool                    `yaml:"url_decode"`
	RawConsentRegex string                  `yaml:"raw_consent_regex"`
	consentRegex    *regexp.Regexp
}

func initConsentCodex(config *appConfig) {
	config.ConsentCodex.consentRegex = regexp.MustCompile(config.ConsentCodex.RawConsentRegex)
}

type consentStatus map[consentLevel]bool

func consentFilter(config *appConfig, status consentStatus) func(string, string) bool {
	return func(k string, v string) bool {
		res := true
		switch {
		case status[config.IDSettings[k].RequiredConsentLevel]:
			res = false
		case v == "[REDACTED]" && config.IDSettings[k].IDRedactionEnabled:
			res = false
		}
		return res
	}
}

func redactIDs(config *appConfig, status consentStatus, ids map[string]string) map[string]string {
	for key, _ := range ids {
		settings, _ := config.IDSettings[key]
		if settings.IDRedactionEnabled && !status[settings.RequiredConsentLevel] {
			ids[key] = "[REDACTED]"
		}
	}
	return ids
}

func parseConsentStatus(config *appConfig, req *http.Request) (consentStatus, error) {
	cookie, err := loadConsentCookie(config, req)
	if err != nil {
		return nil, err
	}

	if !utf8.Valid([]byte(cookie.Value)) {
		return nil, errors.New("consent status value is not valid utf-8")
	}

	status, err := decodeConsentStatus(AppConfiguration.ConsentCodex, cookie.Value)
	if err != nil {
		return nil, err
	}

	return status, nil
}

func encodeConsentStatus(codex *consentCodex, status consentStatus) (string, error) {
	return "", errors.New("not implemented")
}

func decodeConsentStatus(codex *consentCodex, str string) (consentStatus, error) {
	var err error
	str, err = url.QueryUnescape(str)
	if err != nil {
		return nil, err
	}

	if codex.consentRegex != nil {
		rgx := regexp.MustCompile(`groups=(.+?)&`)
		res := rgx.FindStringSubmatch(str)
		str = res[len(res)-1]
	}

	fields := strings.Split(str, codex.FieldDelim)
	if len(fields) == 0 {
		return nil, errors.New("empty consent status")
	}

	status := make(consentStatus, len(fields))
	for i := 0; i < len(fields); i++ {
		kv := strings.Split(fields[i], codex.KeyDelim)
		switch {
		case len(kv) != 2:
			continue
		case kv[0] == "":
			continue
		}

		level, ok := codex.ConsentKeys[kv[0]]
		if !ok {
			continue
		}

		status[level] = kv[1] == codex.TrueValue

	}

	if len(status) == 0 {
		return nil, errors.New("empty consent status")
	}

	return status, nil
}

func loadConsentCookie(config *appConfig, req *http.Request) (*http.Cookie, error) {
	var cookie *http.Cookie
	var err error
	switch {
	case config.PersistConsentEnabled:
		cookie, err = req.Cookie(config.PersistConsentName)
		if err != nil {
			cookie, err = req.Cookie(config.ConsentStatusName)
		}
	default:
		cookie, err = req.Cookie(config.ConsentStatusName)
	}
	return cookie, err
}
