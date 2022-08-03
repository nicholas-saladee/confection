package main

import (
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"context"
	"github.com/segmentio/analytics-go/v3"
	"io/ioutil"
	"log"
	"regexp"
)

type appConfig struct {
	RootDomain string `yaml:"root_domain"`
	Subdomain  string `yaml:"subdomain"`

	IDContainerName      string                `yaml:"id_container_name"`
	IDSettings           map[string]*idSetting `yaml:"id_settings"`
	IDContainerExpInDays int                   `yaml:"id_container_exp_in_days"`

	ConfectionIDEnabled   bool   `yaml:"confection_id_enabled"`
	ConfectionIDName      string `yaml:"confection_id_name"`
	ConfectionIDExpInDays int    `yaml:"confection_id_exp_in_days"`

	ConsentEnabled          bool          `yaml:"consent_enabled"`
	ConsentStatusName       string        `yaml:"consent_status_name"`
	ConsentDefault          consentStatus `yaml:"consent_default"`
	ConsentCodex            *consentCodex `yaml:"consent_codex"`
	PersistConsentEnabled   bool          `yaml:"persist_consent_enabled"`
	PersistConsentName      string        `yaml:"persist_consent_name"`
	PersistConsentExpInDays int           `yaml:"persist_consent_exp_in_days"`

	EnabledPlugins map[plugin]bool `yaml:"enabled_plugins"`
	PluginSettings *pluginSettings `yaml:"plugin_settings"`
}

type idSetting struct {
	RequiredConsentLevel consentLevel `yaml:"required_consent_level"`
	ExpInDays            int          `yaml:"exp_in_days"`
	ShouldHash           bool         `yaml:"should_hash"`
	ValueRegEx           string       `yaml:"value_regex"`
	IDRedactionEnabled   bool         `yaml:"id_redaction_enabled"`
	regX                 *regexp.Regexp
}

var AppConfiguration *appConfig
var JSFile *storage.ObjectHandle
var BQInserter *bigquery.Inserter
var SegmentClient analytics.Client
var messageChan chan *pluginMessage

func initAppConfiguration(bucketName, configFileName string, client *storage.Client) *appConfig {
	bucket := client.Bucket(bucketName)
	cr, err := bucket.Object(configFileName).NewReader(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	body, err := ioutil.ReadAll(cr)
	if err != nil {
		log.Fatal(err)
	}

	var config appConfig
	err = yaml.Unmarshal(body, &config)
	if err != nil {
		log.Fatal(err)
	}

	return &config
}

func initJSFile(bucketName, jsFileName string, client *storage.Client) *storage.ObjectHandle {
	bucket := client.Bucket(bucketName)
	o := bucket.Object(jsFileName)
	return o
}

var pixel = []byte{
	71, 73, 70, 56, 57, 97, 1, 0, 1, 0, 128, 0, 0, 0, 0, 0,
	255, 255, 255, 33, 249, 4, 1, 0, 0, 0, 0, 44, 0, 0, 0, 0,
	1, 0, 1, 0, 0, 2, 1, 68, 0, 59,
}
