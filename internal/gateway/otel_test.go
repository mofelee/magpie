package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yetone/magpie/internal/access"
	"github.com/yetone/magpie/internal/provider"
	"github.com/yetone/magpie/internal/usage"
)

func TestOTelSessionDedupOnlyUnkeyedLoopback(t *testing.T) {
	f := &fake{ctype: "application/json", reply: `{"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5}}`}
	setup(t, provider.Chat, f)
	root := t.TempDir()
	t.Setenv("PI_CODING_AGENT_DIR", root)
	t.Setenv("PI_CODING_AGENT_SESSION_DIR", "")
	dir := filepath.Join(root, "sessions", "work")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "2026-10-03T00-00-00-000Z_session.jsonl")
	if err := os.WriteFile(path, []byte("{\"type\":\"session\",\"id\":\"session\"}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	received := make(chan []byte, 32)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		received <- b
		io.WriteString(w, `{}`)
	}))
	defer collector.Close()
	t.Setenv("MAGPIE_OTEL_ENABLED", "true")
	t.Setenv("MAGPIE_OTEL_ENDPOINT", collector.URL)
	t.Setenv("MAGPIE_OTEL_HEADERS", "")
	t.Setenv("MAGPIE_OTEL_METRICS", "false")
	t.Setenv("MAGPIE_OTEL_SESSIONS", "true")
	stop := usage.StartOTel()
	t.Cleanup(stop)
	b, _ := json.Marshal(map[string]any{"type": "message", "id": "user", "timestamp": time.Now(), "message": map[string]any{"role": "user", "content": "private"}})
	h, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.Write(append(b, '\n')); err != nil {
		t.Fatal(err)
	}
	h.Close()
	deadline := time.Now().Add(6 * time.Second)
	for !usage.OTelSession("pi", "session") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !usage.OTelSession("pi", "session") {
		t.Fatal("local Pi reader unavailable")
	}
	cases := []struct {
		name, addr, key   string
		session, override string
		dedup             bool
	}{
		{"loopback", "127.0.0.1:1234", "", "", "", true},
		{"IPv6 loopback", "[::1]:1234", "", "", "", true},
		{"LAN", "192.168.1.5:1234", "key", "", "", false},
		{"Docker", "172.17.0.2:1234", "key", "", "", false},
		{"WSL", "172.22.0.2:1234", "key", "", "", false},
		{"keyed loopback", "127.0.0.1:1234", "key", "", "", false},
		{"unkeyed remote", "192.168.1.5:1234", "", "", "", false},
		{"visible native session", "127.0.0.1:1234", "", "session", "", true},
		{"WSL mirrored unseen session", "127.0.0.1:1234", "", "wsl-session", "", false},
		{"Docker Desktop unseen session", "127.0.0.1:1234", "", "docker-session", "", false},
		{"visible native overrides routing", "127.0.0.1:1234", "", "session", "routing", true},
		{"unseen native overrides visible routing", "127.0.0.1:1234", "", "wsl-session", "session", false},
		{"visible Magpie session only", "127.0.0.1:1234", "", "", "session", true},
		{"unseen Magpie session only", "127.0.0.1:1234", "", "", "wsl-session", false},
		{"keyed visible session", "127.0.0.1:1234", "key", "session", "", false},
	}
	for i, tc := range cases {
		req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"m1","messages":[{"role":"user","content":"hello"}]}`))
		req.RemoteAddr = tc.addr
		req.Header.Set("User-Agent", "pi/1.0")
		req.Header.Set("X-Session-Id", tc.session)
		req.Header.Set(SessionHeader, tc.override)
		req.Header.Set("traceparent", fmt.Sprintf("00-%032x-%016x-01", i+1, i+1))
		if tc.key != "" {
			req = req.WithContext(access.WithIdentity(req.Context(), access.Identity{KeyID: tc.key}))
		}
		w := httptest.NewRecorder()
		New().Handler().ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("%s: %d %s", tc.name, w.Code, w.Body.String())
		}
	}
	stop()
	counts := map[string]int{}
	for len(received) > 0 {
		var wire struct {
			ResourceSpans []struct {
				ScopeSpans []struct {
					Spans []struct {
						TraceID string `json:"traceId"`
					}
				}
			}
		}
		if err := json.Unmarshal(<-received, &wire); err != nil {
			t.Fatal(err)
		}
		for _, rs := range wire.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				for _, s := range ss.Spans {
					counts[s.TraceID]++
				}
			}
		}
	}
	for i, tc := range cases {
		want := 2
		if tc.dedup {
			want = 0
		}
		if got := counts[fmt.Sprintf("%032x", i+1)]; got != want {
			t.Fatalf("%s: spans=%d want=%d", tc.name, got, want)
		}
	}
	if records := usage.Load(time.Time{}); len(records) != len(cases) {
		t.Fatalf("ledger lost requests: %d", len(records))
	}
}

func TestOTelExportsGatewayUsageWithoutContent(t *testing.T) {
	f := &fake{ctype: "application/json", reply: `{"id":"c1","model":"m1","choices":[{"message":{"role":"assistant","content":"PRIVATE-REPLY"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5}}`}
	setup(t, provider.Chat, f)
	received := make(chan string, 1)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		received <- string(b)
		io.WriteString(w, `{}`)
	}))
	defer collector.Close()
	t.Setenv("MAGPIE_OTEL_ENABLED", "true")
	t.Setenv("MAGPIE_OTEL_ENDPOINT", collector.URL)
	t.Setenv("MAGPIE_OTEL_HEADERS", "")
	t.Setenv("MAGPIE_OTEL_METRICS", "false")
	stop := usage.StartOTel()
	t.Cleanup(stop)
	code, body := post(t, "/v1/chat/completions", `{"model":"m1","messages":[{"role":"user","content":"PRIVATE-PROMPT"}]}`)
	if code != 200 || !strings.Contains(body, "PRIVATE-REPLY") {
		t.Fatalf("gateway: %d %s", code, body)
	}
	stop()
	select {
	case exported := <-received:
		if strings.Contains(exported, "PRIVATE-") || !strings.Contains(exported, `"magpie.route.id"`) || !strings.Contains(exported, `"intValue":"10"`) {
			t.Fatalf("export: %s", exported)
		}
	case <-time.After(time.Second):
		t.Fatal("gateway usage was not exported")
	}
}

// with bodies on (#538) the span carries the request and the reply as
// Langfuse's observation input and output — a stream's text put together,
// a secret in the request still masked — and with it off neither
func TestOTelExportsBodiesWhenOn(t *testing.T) {
	stream := "data: {\"id\":\"c1\",\"model\":\"m1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"PRIVATE-\"}}]}\n\n" +
		"data: {\"id\":\"c1\",\"model\":\"m1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"REPLY\"},\"finish_reason\":\"stop\"}]}\n\n" +
		"data: {\"id\":\"c1\",\"model\":\"m1\",\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\ndata: [DONE]\n\n"
	for _, c := range []struct {
		name, bodies, ctype, reply, request string
	}{
		{"off", "false", "application/json", `{"id":"c1","model":"m1","choices":[{"message":{"role":"assistant","content":"PRIVATE-REPLY"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5}}`, `{"model":"m1","messages":[{"role":"user","content":"PRIVATE-PROMPT"}]}`},
		{"on", "true", "application/json", `{"id":"c1","model":"m1","choices":[{"message":{"role":"assistant","content":"PRIVATE-REPLY"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5}}`, `{"model":"m1","messages":[{"role":"user","content":"PRIVATE-PROMPT"}]}`},
		{"on, streamed", "true", "text/event-stream", stream, `{"model":"m1","stream":true,"messages":[{"role":"user","content":"PRIVATE-PROMPT"}]}`},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &fake{ctype: c.ctype, reply: c.reply}
			setup(t, provider.Chat, f)
			received := make(chan string, 1)
			collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				received <- string(b)
				io.WriteString(w, `{}`)
			}))
			defer collector.Close()
			t.Setenv("MAGPIE_OTEL_ENABLED", "true")
			t.Setenv("MAGPIE_OTEL_ENDPOINT", collector.URL)
			t.Setenv("MAGPIE_OTEL_HEADERS", "")
			t.Setenv("MAGPIE_OTEL_METRICS", "false")
			t.Setenv("MAGPIE_OTEL_BODIES", c.bodies)
			stop := usage.StartOTel()
			t.Cleanup(stop)
			secret := "sk-ant-api03-" + strings.Repeat("Q7x", 30)
			request := strings.Replace(c.request, `"model":"m1"`, `"model":"m1","metadata":{"api_key":"`+secret+`"}`, 1)
			if code, body := post(t, "/v1/chat/completions", request); code != 200 || !strings.Contains(body, "REPLY") {
				t.Fatalf("gateway: %d %s", code, body)
			}
			stop()
			var exported string
			select {
			case exported = <-received:
			case <-time.After(time.Second):
				t.Fatal("gateway usage was not exported")
			}
			if strings.Contains(exported, secret) {
				t.Fatalf("secret exported: %s", exported)
			}
			attrs := map[string]string{}
			var wire struct {
				ResourceSpans []struct {
					ScopeSpans []struct {
						Spans []struct {
							Attributes []struct {
								Key   string            `json:"key"`
								Value map[string]string `json:"value"`
							} `json:"attributes"`
						} `json:"spans"`
					} `json:"scopeSpans"`
				} `json:"resourceSpans"`
			}
			if err := json.Unmarshal([]byte(exported), &wire); err != nil {
				t.Fatal(err)
			}
			for _, a := range wire.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes {
				attrs[a.Key] = a.Value["stringValue"]
			}
			in, out := attrs["langfuse.observation.input"], attrs["langfuse.observation.output"]
			if c.bodies == "false" {
				if in != "" || out != "" || strings.Contains(exported, "PRIVATE-") {
					t.Fatalf("bodies exported while off: %s", exported)
				}
				return
			}
			if !strings.Contains(in, `"content":"PRIVATE-PROMPT"`) {
				t.Fatalf("input: %q", in)
			}
			if c.ctype == "text/event-stream" {
				if out != "PRIVATE-REPLY" {
					t.Fatalf("streamed output: %q", out)
				}
			} else if !strings.Contains(out, `"content":"PRIVATE-REPLY"`) {
				t.Fatalf("output: %q", out)
			}
		})
	}
}

func TestOTelWaterfallIncludesUnbilledFallback(t *testing.T) {
	plan := &fake{ctype: "application/json", code: 429, reply: `{"error":{"message":"PRIVATE-ERROR"}}`}
	spare := &fake{ctype: "application/json", reply: `{"id":"ok","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5}}`}
	twoProviders(t, plan, spare)
	received := make(chan []byte, 4)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		received <- b
		io.WriteString(w, `{}`)
	}))
	defer collector.Close()
	t.Setenv("MAGPIE_OTEL_ENABLED", "true")
	t.Setenv("MAGPIE_OTEL_ENDPOINT", collector.URL)
	t.Setenv("MAGPIE_OTEL_HEADERS", "")
	t.Setenv("MAGPIE_OTEL_METRICS", "true")
	t.Setenv("MAGPIE_OTEL_BODIES", "false")
	stop := usage.StartOTel()
	t.Cleanup(stop)
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(chatReq))
	traceID, parentID := strings.Repeat("a", 32), strings.Repeat("b", 16)
	req.Header.Set("traceparent", "00-"+traceID+"-"+parentID+"-01")
	response := httptest.NewRecorder()
	New().Handler().ServeHTTP(response, req)
	if response.Code != 200 {
		t.Fatalf("gateway: %d %s", response.Code, response.Body.String())
	}
	stop()
	exported := <-received
	if strings.Contains(string(exported), "PRIVATE-") {
		t.Fatalf("private data exported: %s", exported)
	}
	var wire struct {
		ResourceSpans []struct {
			ScopeSpans []struct {
				Spans []struct {
					TraceID    string `json:"traceId"`
					SpanID     string `json:"spanId"`
					ParentID   string `json:"parentSpanId"`
					Start      string `json:"startTimeUnixNano"`
					End        string `json:"endTimeUnixNano"`
					Attributes []struct {
						Key   string
						Value map[string]string
					}
					Status struct{ Code int }
				}
			}
		}
	}
	if err := json.Unmarshal(exported, &wire); err != nil {
		t.Fatal(err)
	}
	spans := wire.ResourceSpans[0].ScopeSpans[0].Spans
	if len(spans) != 3 {
		t.Fatalf("want two attempts and a root: %s", exported)
	}
	root := spans[2]
	if root.TraceID != traceID || root.ParentID != parentID || root.Status.Code != 0 {
		t.Fatalf("root: %+v", root)
	}
	for i, child := range spans[:2] {
		if child.TraceID != traceID || child.ParentID != root.SpanID || child.SpanID == root.SpanID || child.Start < root.Start || child.End > root.End {
			t.Fatalf("child %d: %+v, root %+v", i, child, root)
		}
	}
	if spans[0].Status.Code != 2 || spans[1].Status.Code != 0 {
		t.Fatalf("attempt statuses: %+v", spans)
	}
	for _, a := range root.Attributes {
		if strings.HasPrefix(a.Key, "gen_ai.usage.") {
			t.Fatalf("root duplicates usage: %+v", a)
		}
	}
	// Accounting remains one successful call; failed attempts are telemetry only.
	if records := usage.Load(time.Time{}); len(records) != 1 || records[0].Input != 10 {
		t.Fatalf("ledger: %+v", records)
	}
	// Metric histograms count two attempts, not the request parent.
	metrics := string(<-received)
	if strings.Contains(metrics, `"stringValue":""`) {
		t.Fatalf("root included in metrics: %s", metrics)
	}
}

func TestOTelParentValidation(t *testing.T) {
	valid := "00-" + strings.Repeat("a", 32) + "-" + strings.Repeat("b", 16) + "-01"
	for _, value := range []string{valid, "", strings.Replace(valid, "00-", "ff-", 1), strings.ToUpper(valid), strings.Replace(valid, strings.Repeat("a", 32), strings.Repeat("0", 32), 1), valid + "-extra", valid[:len(valid)-1] + "z"} {
		trace, parent := otelParent(value)
		if value == valid {
			if trace == "" || parent == "" {
				t.Fatal("valid parent rejected")
			}
		} else if trace != "" || parent != "" {
			t.Fatalf("accepted malformed parent: %q", value)
		}
	}
}

func TestOTelWaterfallFailureAndCancellation(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		name := "failure"
		if canceled {
			name = "canceled"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f := &fake{ctype: "application/json", code: 400, reply: `{"error":{"message":"invalid request"}}`}
			if canceled {
				f.refuse = func([]byte) (int, string) { cancel(); return 400, f.reply }
			}
			setup(t, provider.Chat, f)
			received := make(chan []byte, 4)
			collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				received <- b
				io.WriteString(w, `{}`)
			}))
			defer collector.Close()
			t.Setenv("MAGPIE_OTEL_ENABLED", "true")
			t.Setenv("MAGPIE_OTEL_ENDPOINT", collector.URL)
			t.Setenv("MAGPIE_OTEL_HEADERS", "")
			t.Setenv("MAGPIE_OTEL_METRICS", "false")
			t.Setenv("MAGPIE_OTEL_BODIES", "false")
			stop := usage.StartOTel()
			t.Cleanup(stop)
			req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"m1","messages":[{"role":"user","content":"hi"}]}`)).WithContext(ctx)
			New().Handler().ServeHTTP(httptest.NewRecorder(), req)
			stop()
			select {
			case b := <-received:
				if strings.Count(string(b), `"code":2`) != 2 || strings.Count(string(b), `"parentSpanId"`) != 1 {
					t.Fatalf("expected failed child and root: %s", b)
				}
				if canceled && !strings.Contains(string(b), `"intValue":"499"`) {
					t.Fatalf("cancellation status missing: %s", b)
				}
			case <-time.After(time.Second):
				t.Fatal("no failure trace")
			}
		})
	}
}

// with bodies whole (#538) the export carries the request and reply entire,
// not the 256 KiB the Recent-calls page keeps, and no cut mark; with it off
// the first 256 KiB of each goes, cut, as before
func TestOTelExportsWholeBodiesWhenOn(t *testing.T) {
	big := strings.Repeat("A", 300<<10)
	reply := `{"id":"c1","model":"m1","choices":[{"message":{"role":"assistant","content":"` + big + `"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5}}`
	request := `{"model":"m1","messages":[{"role":"user","content":"` + big + `"}]}`
	for _, c := range []struct {
		name   string
		whole  bool
		cut    bool
		stream bool
	}{
		{"cut", false, true, false},
		{"whole", true, false, false},
		{"cut streamed", false, true, true},
		{"whole streamed", true, false, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &fake{ctype: "application/json", reply: reply}
			if c.stream {
				f.ctype = "text/event-stream"
				f.reply = "data: {\"choices\":[{\"delta\":{\"content\":\"" + big + "\"}}]}\n\ndata: [DONE]\n\n"
			}
			setup(t, provider.Chat, f)
			received := make(chan string, 1)
			collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				received <- string(b)
				io.WriteString(w, `{}`)
			}))
			defer collector.Close()
			t.Setenv("MAGPIE_OTEL_ENABLED", "true")
			t.Setenv("MAGPIE_OTEL_ENDPOINT", collector.URL)
			t.Setenv("MAGPIE_OTEL_HEADERS", "")
			t.Setenv("MAGPIE_OTEL_METRICS", "false")
			t.Setenv("MAGPIE_OTEL_BODIES", "true")
			t.Setenv("MAGPIE_OTEL_BODIES_WHOLE", fmt.Sprintf("%t", c.whole))
			stop := usage.StartOTel()
			t.Cleanup(stop)
			s := New()
			response := httptest.NewRecorder()
			s.Handler().ServeHTTP(response, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(request)))
			if response.Code != 200 || !strings.Contains(response.Body.String(), big) {
				t.Fatalf("gateway: %d", response.Code)
			}
			recent := s.Recent()
			if len(recent) != 1 {
				t.Fatalf("recent calls: %d, want 1", len(recent))
			}
			if recent[0].otelIn != nil || recent[0].otelOut != nil {
				t.Fatal("Recent calls retained whole OTel bodies")
			}
			if len(recent[0].RequestBody) != callBodyLimit || len(recent[0].ResponseBody) != callBodyLimit || !recent[0].RequestTruncated || !recent[0].ResponseTruncated {
				t.Fatal("Recent calls must retain only the truncated diagnostic bodies")
			}
			stop()
			var exported string
			select {
			case exported = <-received:
			case <-time.After(3 * time.Second):
				t.Fatal("gateway usage was not exported")
			}
			attrs := otelSpanAttrs(t, exported)
			in, out := attrs["langfuse.observation.input"], attrs["langfuse.observation.output"]
			if c.cut {
				if !strings.Contains(in, usage.BodyCut) || !strings.Contains(out, usage.BodyCut) {
					t.Fatalf("expected cut bodies, got in=%d out=%d", len(in), len(out))
				}
				return
			}
			if strings.Contains(in, usage.BodyCut) || strings.Contains(out, usage.BodyCut) {
				t.Fatalf("whole bodies were cut")
			}
			if len(in) < len(big) || len(out) < len(big) {
				t.Fatalf("short bodies: in=%d out=%d want at least %d", len(in), len(out), len(big))
			}
		})
	}
}

// otelSpanAttrs is the first span's string attributes in one exported batch.
func otelSpanAttrs(t *testing.T, exported string) map[string]string {
	t.Helper()
	var wire struct {
		ResourceSpans []struct {
			ScopeSpans []struct {
				Spans []struct {
					Attributes []struct {
						Key   string            `json:"key"`
						Value map[string]string `json:"value"`
					} `json:"attributes"`
				} `json:"spans"`
			} `json:"scopeSpans"`
		} `json:"resourceSpans"`
	}
	if err := json.Unmarshal([]byte(exported), &wire); err != nil {
		t.Fatal(err)
	}
	attrs := map[string]string{}
	for _, a := range wire.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes {
		attrs[a.Key] = a.Value["stringValue"]
	}
	return attrs
}

// Each attempt keeps its own complete body across fallback.
func TestOTelWholeWaterfallFallback(t *testing.T) {
	big := strings.Repeat("A", 300<<10)
	secret := "sk-ant-api03-" + strings.Repeat("Q7x", 30)
	plan := &fake{ctype: "application/json", code: 429, reply: `{"error":{"message":"` + big + `"}}`}
	spare := &fake{ctype: "application/json", reply: `{"choices":[{"message":{"content":"` + big + `"}}],"usage":{"prompt_tokens":10,"completion_tokens":5}}`}
	twoProviders(t, plan, spare)
	received := make(chan string, 4)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		received <- string(b)
		io.WriteString(w, `{}`)
	}))
	defer collector.Close()
	t.Setenv("MAGPIE_OTEL_ENABLED", "true")
	t.Setenv("MAGPIE_OTEL_ENDPOINT", collector.URL)
	t.Setenv("MAGPIE_OTEL_HEADERS", "")
	t.Setenv("MAGPIE_OTEL_METRICS", "false")
	t.Setenv("MAGPIE_OTEL_BODIES", "true")
	t.Setenv("MAGPIE_OTEL_BODIES_WHOLE", "true")
	stop := usage.StartOTel()
	t.Cleanup(stop)
	request := `{"model":"plan/m1","messages":[{"role":"user","content":"` + big + `"}],"metadata":{"api_key":"` + secret + `"}}`
	if code, _ := post(t, "/v1/chat/completions", request); code != 200 {
		t.Fatalf("gateway: %d", code)
	}
	stop()
	exported := ""
	for len(received) > 0 {
		exported += <-received + "\n"
	}
	if strings.Contains(exported, secret) {
		t.Fatal("secret after capture limit leaked")
	}
	var wire struct {
		ResourceSpans []struct {
			ScopeSpans []struct {
				Spans []struct {
					ParentID   string `json:"parentSpanId"`
					SpanID     string `json:"spanId"`
					Attributes []struct {
						Key   string
						Value map[string]string
					}
				}
			}
		}
	}
	var spans []struct {
		ParentID   string `json:"parentSpanId"`
		SpanID     string `json:"spanId"`
		Attributes []struct {
			Key   string
			Value map[string]string
		}
	}
	for _, batch := range strings.Split(strings.TrimSpace(exported), "\n") {
		if err := json.Unmarshal([]byte(batch), &wire); err != nil {
			t.Fatal(err)
		}
		spans = append(spans, wire.ResourceSpans[0].ScopeSpans[0].Spans...)
	}
	if len(spans) != 3 {
		t.Fatalf("spans: %d", len(spans))
	}
	for i, span := range spans[:2] {
		attrs := map[string]string{}
		for _, a := range span.Attributes {
			attrs[a.Key] = a.Value["stringValue"]
		}
		for _, key := range []string{"langfuse.observation.input", "langfuse.observation.output"} {
			value := attrs[key]
			if !strings.Contains(value, big) || strings.Contains(value, usage.BodyCut) {
				t.Fatalf("attempt %d %s incomplete (%d bytes)", i, key, len(value))
			}
		}
		if span.ParentID != spans[2].SpanID {
			t.Fatal("wrong waterfall parent")
		}
	}
}
