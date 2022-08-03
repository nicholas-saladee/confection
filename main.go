package main

import (
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/segmentio/analytics-go/v3"
	"github.com/segmentio/ksuid"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
	"unicode/utf8"
)

func init() {
	bucket := os.Getenv("BUCKET_NAME")
	configFileName := os.Getenv("CONFIG_NAME")
	jsFileName := os.Getenv("JS_NAME")

	switch {
	case bucket == "":
		log.Fatal("empty bucket name")
	case configFileName == "":
		log.Fatal("empty config file name")
	case jsFileName == "":
		log.Fatal("empty js file name")
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}

	AppConfiguration = initAppConfiguration(bucket, configFileName, client)
	JSFile = initJSFile(bucket, jsFileName, client)

	initConsentCodex(AppConfiguration)

	bq, err := bigquery.NewClient(ctx, "t-cloud-run")
	if err != nil {
		log.Fatal(err)
	}

	dataset := bq.Dataset(AppConfiguration.PluginSettings.BQDataset)
	table := dataset.Table(AppConfiguration.PluginSettings.BQTable)
	BQInserter = table.Inserter()

	SegmentClient = analytics.New(AppConfiguration.PluginSettings.SegmentWriteKey)

	messageChan = make(chan *pluginMessage)
	go procPlugins(messageChan)
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	http.HandleFunc("/load", APIWrap(Load, nil))
	http.HandleFunc("/update", APIWrap(Update, messageChan))
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
}

func decodeContainer(str string) (map[string]string, error) {
	if str == "" {
		return nil, errors.New("")
	}
	kvs := strings.Split(str, "&")

	if len(kvs) == 0 {
		return nil, errors.New("empty id container")
	}

	out := make(map[string]string, len(kvs))
	for i := 0; i < len(kvs); i++ {
		kv := strings.Split(kvs[i], ":")
		switch {
		case len(kv) != 2:
			continue
		case kv[0] == "" || kv[1] == "":
			continue
		}
		out[kv[0]] = kv[1]
	}

	return out, nil
}

func encodeContainer(ids map[string]string) (string, error) {
	if ids == nil || len(ids) == 0 {
		return "", errors.New("")
	}

	i := 0
	end := len(ids) - 1
	var builder strings.Builder
	for k, v := range ids {
		builder.WriteString(k)
		builder.WriteByte(':')
		builder.WriteString(v)
		if i != end {
			builder.WriteByte('&')
			i++
		}
	}

	return builder.String(), nil
}

func filterIDs(config *appConfig, ids map[string]string, filter func(string, string) bool) map[string]string {
	do := filter != nil
	for k, v := range ids {
		_, ok := config.IDSettings[k]
		switch {
		case !ok:
			delete(ids, k)
		case do && filter(k, v):
			delete(ids, k)
		}
	}
	return ids
}

func loadIDs(config *appConfig, r *http.Request) (map[string]string, error) {
	container, err := r.Cookie(config.IDContainerName)
	switch {
	case err != nil:
		return nil, err
	case container.Value == "":
		return nil, errors.New("")
	case !utf8.Valid([]byte(container.Value)):
		return nil, errors.New("")
	}

	ids, err := decodeContainer(container.Value)
	return ids, nil
}

func makeServerCookie(config *appConfig, name, value string, exp int) *http.Cookie {
	cookie := new(http.Cookie)
	cookie.Name = name
	cookie.Value = value
	cookie.Expires = time.Now().Add(time.Duration(exp * 24 * int(time.Hour)))
	cookie.Domain = config.Subdomain + "." + config.RootDomain
	cookie.Path = "/"
	cookie.Secure = true
	cookie.HttpOnly = true
	cookie.SameSite = http.SameSiteStrictMode

	return cookie
}

func makeJSCookie(config *appConfig, name, value string, exp int) *http.Cookie {
	cookie := new(http.Cookie)
	cookie.Name = name
	cookie.Value = value
	cookie.Expires = time.Now().Add(time.Duration(exp * 24 * int(time.Hour)))
	cookie.Domain = config.Subdomain + "." + config.RootDomain
	cookie.Path = "/"
	cookie.Secure = true
	cookie.HttpOnly = false
	cookie.SameSite = http.SameSiteLaxMode

	return cookie
}

func makeJSCookies(config *appConfig, ids map[string]string) []*http.Cookie {
	// make js readable cookies
	out := make([]*http.Cookie, 0, len(ids))
	for name, value := range ids {
		// set js readable cookie
		out = append(out, &http.Cookie{
			Name:     name,
			Value:    value,
			Path:     "/",
			Domain:   "." + config.RootDomain,
			Expires:  time.Now().Add(time.Duration(config.IDSettings[name].ExpInDays * 24 * int(time.Hour))),
			Secure:   true,
			HttpOnly: false,
			SameSite: http.SameSiteLaxMode,
		})
	}

	return out
}

func APIWrap(method func(http.ResponseWriter, *http.Request, chan<- *pluginMessage), out chan<- *pluginMessage) func(http.ResponseWriter, *http.Request) {
	return func(writer http.ResponseWriter, request *http.Request) {
		method(writer, request, out)
	}
}

func Load(wr http.ResponseWriter, req *http.Request, out chan<- *pluginMessage) {
	var cookies []*http.Cookie
	var status consentStatus

	ids, err := loadIDs(AppConfiguration, req)
	if err != nil {
		goto sendFile
	}

	cookies = make([]*http.Cookie, 0, len(ids)+3)
	if AppConfiguration.ConsentEnabled {
		usedDefault := false
		status, err = parseConsentStatus(AppConfiguration, req)
		if err != nil {
			status = AppConfiguration.ConsentDefault
			if status == nil {
				log.Fatal("unable to parse consent and default not set")
			}
			usedDefault = true
		}

		ids = redactIDs(AppConfiguration, status, ids)
		ids = filterIDs(AppConfiguration, ids, consentFilter(AppConfiguration, status))

		if AppConfiguration.PersistConsentEnabled && usedDefault == false {
			str, err := encodeConsentStatus(AppConfiguration.ConsentCodex, status)
			if err != nil {
				goto setCookies
			}

			cookies = append(cookies, makeJSCookie(
				AppConfiguration,
				AppConfiguration.ConsentStatusName,
				str,
				AppConfiguration.PersistConsentExpInDays,
			))

			cookies = append(cookies, makeServerCookie(
				AppConfiguration,
				AppConfiguration.PersistConsentName,
				str,
				AppConfiguration.PersistConsentExpInDays,
			))
		}
	}

setCookies:
	if len(ids) != 0 {
		cookies = append(cookies, makeJSCookies(AppConfiguration, ids)...)
	}

	if AppConfiguration.ConfectionIDEnabled {
		var str string
		cid, err := req.Cookie(AppConfiguration.ConfectionIDName)
		switch {
		case err != nil:
			str = ksuid.New().String()
		default:
			str = cid.Value
		}

		ids[AppConfiguration.ConfectionIDName] = str
		cookies = append(cookies, makeServerCookie(
			AppConfiguration,
			AppConfiguration.ConfectionIDName,
			str,
			AppConfiguration.ConfectionIDExpInDays,
		))
	}

	if cookies != nil && len(cookies) != 0 {
		for i := 0; i < len(cookies); i++ {
			wr.Header().Add("Set-Cookie", cookies[i].String())
		}
	}

	if out != nil && len(ids) != 0 {
		out <- &pluginMessage{
			destinations: map[plugin]bool{
				SegmentIdentify: true,
				BQStore:         true,
			},
			req:    req,
			ids:    ids,
			status: status,
		}
	}

sendFile:
	ctx := context.TODO()
	reader, err := JSFile.NewReader(ctx)
	if err != nil {
		wr.WriteHeader(http.StatusFailedDependency)
		_, err := fmt.Fprintln(wr, err)
		if err != nil {
			log.Fatal(err)
		}
		return
	}

	body, err := ioutil.ReadAll(reader)
	if err != nil {
		wr.WriteHeader(http.StatusFailedDependency)
		_, err := fmt.Fprintln(wr, err)
		if err != nil {
			log.Fatal(err)
		}
		return
	}

	wr.WriteHeader(http.StatusOK)
	wr.Header().Set("Content-Type", "text/javascript")
	wr.Header().Add("Cache-Control", "no-cache, no-store, must-revalidate")
	_, err = wr.Write(body)
	if err != nil {
		wr.WriteHeader(http.StatusFailedDependency)
		_, err := fmt.Fprintln(wr, err)
		if err != nil {
			log.Fatal(err)
		}
		return
	}

	err = reader.Close()
	if err != nil {
		log.Fatal(err)
	}

	return
}

func Update(wr http.ResponseWriter, req *http.Request, out chan<- *pluginMessage) {
	if !utf8.Valid([]byte(req.URL.RawQuery)) {
		fmt.Fprintln(wr, fmt.Sprintf("query of %s is not valid", req.URL.RawQuery))
		return
	}

	query := req.URL.Query()
	if query == nil {
		fmt.Fprintln(wr, "empty update")
		return
	}

	var existing map[string]string
	existing, err := loadIDs(AppConfiguration, req)
	if existing == nil || err != nil {
		existing = make(map[string]string)
	}

	var cookies []*http.Cookie
	var status consentStatus
	existing, err = updateIDs(AppConfiguration, existing, query)

	if AppConfiguration.ConsentEnabled {
		usedDefault := false
		status, err = parseConsentStatus(AppConfiguration, req)
		if err != nil {
			status = AppConfiguration.ConsentDefault
			if status == nil {
				log.Fatal("unable to parse consent and default not set")
			}
			usedDefault = true
		}

		existing = filterIDs(AppConfiguration, existing, consentFilter(AppConfiguration, status))

		if AppConfiguration.PersistConsentEnabled && usedDefault == false {
			str, err := encodeConsentStatus(AppConfiguration.ConsentCodex, status)
			if err != nil {
				goto setCookies
			}

			cookies = append(cookies, makeJSCookie(
				AppConfiguration,
				AppConfiguration.ConsentStatusName,
				str,
				AppConfiguration.PersistConsentExpInDays,
			))

			cookies = append(cookies, makeServerCookie(
				AppConfiguration,
				AppConfiguration.PersistConsentName,
				str,
				AppConfiguration.PersistConsentExpInDays,
			))
		}
	}

setCookies:
	if len(existing) != 0 {
		str, _ := encodeContainer(existing)
		cookies = append(cookies, makeServerCookie(
			AppConfiguration,
			AppConfiguration.IDContainerName,
			str,
			AppConfiguration.IDContainerExpInDays,
		))
		cookies = append(cookies, makeJSCookies(AppConfiguration, existing)...)
	}

	if AppConfiguration.ConfectionIDEnabled {
		var str string
		cid, err := req.Cookie(AppConfiguration.ConfectionIDName)
		switch {
		case err != nil:
			str = ksuid.New().String()
		default:
			str = cid.Value
		}

		existing[AppConfiguration.ConfectionIDName] = str
		cookies = append(cookies, makeServerCookie(
			AppConfiguration,
			AppConfiguration.ConfectionIDName,
			str,
			AppConfiguration.ConfectionIDExpInDays,
		))
	}

	if out != nil && len(existing) != 0 {
		out <- &pluginMessage{
			destinations: map[plugin]bool{
				SegmentIdentify: true,
				BQStore:         true,
			},
			req:    req,
			ids:    existing,
			status: status,
		}
	}

	if cookies != nil && len(cookies) != 0 {
		for i := 0; i < len(cookies); i++ {
			wr.Header().Add("Set-Cookie", cookies[i].String())
		}
	}

	wr.Header().Add("Content-Type", "image/gif")
	wr.Header().Add("Cache-Control", "no-cache, no-store, must-revalidate")
	wr.WriteHeader(http.StatusOK)
	_, err = wr.Write(pixel)
	if err != nil {
		wr.WriteHeader(http.StatusFailedDependency)
		_, err := fmt.Fprintln(wr, err)
		if err != nil {
			log.Fatal(err)
		}
		return
	}

	return
}

func updateIDs(config *appConfig, existing map[string]string, updates url.Values) (map[string]string, error) {
	switch {
	case updates.Has("set"):
		toSet := updates["set"]
		if len(toSet) == 0 {
			goto completeSet
		}

		for i := 0; i < len(toSet); i++ {
			kv := strings.Split(toSet[i], ":")
			switch {
			case len(kv) != 2:
				continue
			case kv[0] == "" || kv[1] == "":
				continue
			}

			// check if valid id
			setting, ok := config.IDSettings[kv[0]]
			switch {
			case !ok:
				continue
			case setting.regX != nil:
				if !setting.regX.MatchString(kv[1]) {
					continue
				}
				fallthrough
			case setting.ShouldHash:
				sum := sha256.Sum256([]byte(kv[1]))
				kv[1] = hex.EncodeToString(sum[:])
			}

			existing[kv[0]] = kv[1]
		}
	completeSet:
		fallthrough
	case updates.Has("delete"):
		toDelete := updates["delete"]
		if len(toDelete) == 0 {
			goto completeDelete
		}

		for i := 0; i < len(toDelete); i++ {
			if toDelete[i] == "" {
				continue
			}
			delete(existing, toDelete[i])
		}
	completeDelete:
		fallthrough
	default:

	}

	switch {
	case len(existing) == 0:
		return nil, errors.New("")
	}

	return existing, nil
}
