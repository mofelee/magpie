package usage

import (
	"context"
	"reflect"
	"time"

	"github.com/yetone/magpie/internal/redact"
	"github.com/yetone/magpie/internal/sessions"
	"github.com/yetone/magpie/internal/settings"
)

type sessionAvailability struct {
	config settings.OTel
	agents map[string]time.Time
	seen   map[observedSession]time.Time
}

type observedSession struct {
	agent, id string
}

// OTelSessionAgent reports recent observations for callers without a session ID.
func OTelSessionAgent(agent string) bool { return OTelSession(agent, "") }

// OTelSession reports whether this exact agent/session has recent local
// observations. Only requests without an ID fall back to agent-level readiness.
// Callers must also verify loopback and absence of a gateway key.
func OTelSession(agent, session string) bool {
	e := otel.Load()
	if e == nil || !sessions.TraceAgent(agent) {
		return false
	}
	config, err := settings.OTelExport()
	return err == nil && e.sessionObserved(agent, session, config)
}

func (e *otelExporter) sessionObserved(agent, session string, config settings.OTel) bool {
	a := e.sessions.Load()
	if !config.Enabled || !config.Sessions || a == nil || !reflect.DeepEqual(a.config, config) {
		return false
	}
	at := a.agents[agent]
	if session != "" {
		at = a.seen[observedSession{agent, session}]
	}
	return time.Since(at) < 5*time.Minute
}

func sessionBody(body string, config settings.OTel) string {
	if !config.Bodies {
		return ""
	}
	body = string(redact.ScrubJSON([]byte(body)))
	cut := false
	if !config.BodiesWhole && len(body) > 256<<10 {
		body = body[:256<<10]
		cut = true
	}
	if cut {
		body += BodyCut
	}
	return body
}
func sessionRecord(s sessions.TraceSpan, config settings.OTel) Record {
	status := 200
	if s.Error {
		status = 500
	}
	typ := s.Kind
	return Record{Agent: s.Agent, Model: s.Model, Served: s.Served, Provider: s.Provider, Time: s.Start, Millis: s.End.Sub(s.Start).Milliseconds(), Status: status,
		Input: s.Tokens.Input, Output: s.Tokens.Output, CacheRead: s.Tokens.CacheRead, CacheWrite: s.Tokens.CacheWrite, Reasoning: s.Reasoning,
		BodyIn: sessionBody(s.Input, config), BodyOut: sessionBody(s.Output, config),
		OTel: &OTelSpan{TraceID: sessions.TraceID(s.Agent, s.Session, s.Turn), SpanID: s.ID, ParentID: s.Parent, Root: s.Parent == "", End: s.End,
			Name: s.Name, Type: typ, SessionID: s.Session, TraceName: s.Agent + " interaction", Session: true, Inferred: s.Inferred, Update: s.Update}}
}

func (e *otelExporter) watchSessions(ctx context.Context, started time.Time) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	var reader *sessions.TraceReader
	var prior settings.OTel
	if config, err := settings.OTelExport(); err == nil && config.Enabled && config.Sessions {
		reader = sessions.NewTraceReader(started)
		prior = config
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		config, err := settings.OTelExport()
		if err != nil || !config.Enabled || !config.Sessions {
			reader = nil
			e.sessions.Store(nil)
			continue
		}
		if reader == nil || !reflect.DeepEqual(config, prior) {
			reader = sessions.NewTraceReader(time.Now())
			prior = config
			e.sessions.Store(nil)
		}
		spans := reader.Poll(config.Bodies)
		available := &sessionAvailability{config: config, agents: map[string]time.Time{}, seen: map[observedSession]time.Time{}}
		if previous := e.sessions.Load(); previous != nil {
			for agent, at := range previous.agents {
				if time.Since(at) < 5*time.Minute {
					available.agents[agent] = at
				}
			}
			for key, at := range previous.seen {
				if time.Since(at) < 5*time.Minute {
					available.seen[key] = at
				}
			}
		}
		for _, s := range spans {
			if s.End.After(available.agents[s.Agent]) {
				available.agents[s.Agent] = s.End
			}
			key := observedSession{s.Agent, s.Session}
			if s.Session != "" && s.End.After(available.seen[key]) {
				available.seen[key] = s.End
			}
		}
		e.sessions.Store(available)
		for _, s := range spans {
			if ctx.Err() != nil {
				return
			}
			e.offer(otelItem{record: sessionRecord(s, config), config: config})
		}
	}
}
