package sinks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/resmoio/kubernetes-event-exporter/pkg/kube"
	"github.com/rs/zerolog/log"
)

type promtailStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

type LokiMsg struct {
	Streams []promtailStream `json:"streams"`
}

type LokiConfig struct {
	Layout           map[string]interface{} `yaml:"layout"`
	StreamLabels     map[string]string      `yaml:"streamLabels"`
	TLS              TLS                    `yaml:"tls"`
	URL              string                 `yaml:"url"`
	Headers          map[string]string      `yaml:"headers"`
	IgnoreNamespaces []string               `yaml:"ignore_namespaces"`
}

type Loki struct {
	cfg       *LokiConfig
	transport *http.Transport
}

func NewLoki(cfg *LokiConfig) (Sink, error) {
	tlsClientConfig, err := setupTLS(&cfg.TLS)
	if err != nil {
		return nil, fmt.Errorf("failed to setup TLS: %w", err)
	}
	return &Loki{cfg: cfg, transport: &http.Transport{
		Proxy:           http.ProxyFromEnvironment,
		TLSClientConfig: tlsClientConfig,
	}}, nil
}

func generateTimestamp() string {
	return strconv.FormatInt(time.Now().Unix(), 10) + "000000000"
}

func convertStreamTemplate(layout map[string]string, ev *kube.EnhancedEvent) (map[string]string, error) {
	result := make(map[string]string)
	for key, value := range layout {
		rendered, err := GetString(ev, value)
		if err != nil {
			return nil, err
		}

		result[key] = rendered
	}
	return result, nil
}

func (l *Loki) Send(ctx context.Context, ev *kube.EnhancedEvent) error {
	if l.cfg.Layout == nil {
		l.cfg.Layout = make(map[string]interface{})
	}
	if l.cfg.StreamLabels == nil {
		l.cfg.StreamLabels = make(map[string]string)
	}
	if l.cfg.Headers == nil {
		l.cfg.Headers = make(map[string]string)
	}
	if ev.InvolvedObject.Kind == "Node" {
		l.cfg.StreamLabels["host"] = ev.InvolvedObject.Name
		delete(l.cfg.Layout, "name")
	} else {
		l.cfg.Layout["name"] = "{{ .InvolvedObject.Name }}"
	}

	eventBody, err := serializeEventWithLayout(l.cfg.Layout, ev)
	if err != nil {
		return err
	}

	for _, namespace := range l.cfg.IgnoreNamespaces {
		if namespace == ev.InvolvedObject.Namespace {
			log.Debug().Msgf("Skipping %s namespace, because it is in ignore list", ev.InvolvedObject.Namespace)
			return nil
		}
	}

	if ev.InvolvedObject.Namespace != "" {
		l.cfg.StreamLabels["namespace"] = ev.InvolvedObject.Namespace
		l.cfg.StreamLabels["index"] = l.cfg.StreamLabels["cluster"] + "-" + ev.InvolvedObject.Namespace
	}

	streamLabels, err := convertStreamTemplate(l.cfg.StreamLabels, ev)
	if err != nil {
		return err
	}

	timestamp := generateTimestamp()
	a := LokiMsg{
		Streams: []promtailStream{{
			Stream: streamLabels,
			Values: [][]string{{timestamp, string(eventBody)}},
		}},
	}
	reqBody, err := json.Marshal(a)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, l.cfg.URL, bytes.NewBuffer(reqBody))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	for k, v := range l.cfg.Headers {
		realValue, err := GetString(ev, v)
		if err != nil {
			log.Debug().Err(err).Msgf("parse template failed: %s", v)
			req.Header.Add(k, v)
		} else {
			log.Debug().Msgf("request header: {%s: %s}", k, realValue)
			req.Header.Add(k, realValue)
		}
	}

	client := http.DefaultClient
	client.Transport = l.transport
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	delete(l.cfg.StreamLabels, "namespace")
	delete(l.cfg.StreamLabels, "index")
	delete(l.cfg.StreamLabels, "host")

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if !(resp.StatusCode >= 200 && resp.StatusCode < 300) {
		return errors.New("not successfull (2xx) response: " + string(body))
	}

	return nil
}

func (l *Loki) Close() {
	l.transport.CloseIdleConnections()
}
