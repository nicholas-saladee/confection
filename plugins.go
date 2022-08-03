package main

import (
	"cloud.google.com/go/bigquery"
	"context"
	"github.com/segmentio/analytics-go/v3"
	"log"
	"net/http"
	"net/url"
	"time"
)

type pluginSettings struct {
	SegmentWriteKey string `yaml:"segment_write_key"`
	BQDataset       string `yaml:"bq_dataset"`
	BQTable         string `yaml:"bq_table"`
}

type plugin string

const (
	_               plugin = ""
	SegmentIdentify        = "SEGMENT_IDENTIFY"
	BQStore                = "BQ_STORE"
)

type pluginMessage struct {
	destinations map[plugin]bool
	req          *http.Request
	ids          map[string]string
	status       consentStatus
}

func procPlugins(in <-chan *pluginMessage) {
	for {
		select {
		case message, ok := <-in:
			if !ok {
				//goto end
			}

			ok, _ = message.destinations[BQStore]
			if ok {
				writeToBQ(message)
			}

			ok, _ = message.destinations[SegmentIdentify]
			if ok {
				segmentIdentify(message)
			}
		}
	}
	//end:
	return
}

func segmentIdentify(message *pluginMessage) {
	call := analytics.Identify{
		AnonymousId: message.ids["ajs_anonymous_id"],
		UserId:      message.ids["ajs_user_id"],
		Timestamp:   time.Now(),
		Context:     buildSegmentContext(message.req),
		Traits: map[string]interface{}{
			"ga_client_id": message.ids["_ga"],
			"email":        message.ids["email"],
			"fbp":          message.ids["_fbp"],
		},
		Integrations: map[string]interface{}{
			"Google Analytics": map[string]interface{}{
				"clientId": message.ids["_ga"],
			},
			"Facebook Conversions API (Actions)": map[string]interface{}{
				"_fbp": message.ids["_fbp"],
			},
		},
	}
	err := SegmentClient.Enqueue(call)
	if err != nil {
		log.Print(err)
	}
}

func buildSegmentContext(req *http.Request) *analytics.Context {
	parsed, _ := url.Parse(req.Header.Get("Referer"))
	queryVals := parsed.Query()
	return &analytics.Context{
		Campaign: analytics.CampaignInfo{
			Name:    queryVals.Get("utm_name"),
			Source:  queryVals.Get("utm_source"),
			Medium:  queryVals.Get("utm_medium"),
			Term:    queryVals.Get("utm_term"),
			Content: queryVals.Get("utm_content"),
		},
		Page: analytics.PageInfo{
			Path:   parsed.Path,
			Search: parsed.RawQuery,
			URL:    parsed.String(),
		},
		UserAgent: req.UserAgent(),
	}
}

type idContainer struct {
	ids    map[string]string
	status consentStatus
}

func (i *idContainer) Save() (map[string]bigquery.Value, string, error) {
	tmp := make(map[string]bigquery.Value, len(i.ids))
	for k, v := range i.ids {
		tmp[k] = bigquery.Value(v)
	}

	tmp["functional"] = i.status[Functional]
	tmp["performance"] = i.status[Performance]
	tmp["targeting"] = i.status[Targeting]

	return tmp, bigquery.NoDedupeID, nil
}

func writeToBQ(message *pluginMessage) {
	err := BQInserter.Put(context.TODO(), &idContainer{
		ids:    message.ids,
		status: message.status,
	})
	if err != nil {
		log.Fatal(err)
	}
}
