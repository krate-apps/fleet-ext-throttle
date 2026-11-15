// Command throttle-worker manages named rTorrent throttle profiles via the public SDK.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
)

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      any             `json:"id"`
}

type profile struct {
	Name     string `json:"name"`
	Label    string `json:"label,omitempty"`
	UpRate   int64  `json:"upRate"`
	DownRate int64  `json:"downRate"`
}

type config struct {
	Profiles []profile `json:"profiles"`
}

var (
	mu       sync.Mutex
	nextHost = int64(1)
	pending  = map[int64]chan hostReply{}
	outMu    sync.Mutex
)

type hostReply struct {
	result json.RawMessage
	err    string
}

func main() {
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 0, 64*1024), 8<<20)
	for in.Scan() {
		line := in.Bytes()
		if len(line) == 0 {
			continue
		}
		var probe map[string]json.RawMessage
		if err := json.Unmarshal(line, &probe); err != nil {
			continue
		}
		if _, hasMethod := probe["method"]; !hasMethod {
			var resp struct {
				ID     any             `json:"id"`
				Result json.RawMessage `json:"result"`
				Error  *struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(line, &resp); err != nil {
				continue
			}
			idNum, ok := asInt64(resp.ID)
			if !ok {
				continue
			}
			mu.Lock()
			ch := pending[idNum]
			delete(pending, idNum)
			mu.Unlock()
			if ch == nil {
				continue
			}
			msg := ""
			if resp.Error != nil {
				msg = resp.Error.Message
			}
			ch <- hostReply{result: resp.Result, err: msg}
			continue
		}

		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			continue
		}
		switch req.Method {
		case "fleet.handshake", "fleet.ping", "ext.activate", "ext.deactivate", "ext.health", "ext.upgrade":
			reply(req.ID, map[string]any{"ok": true})
		case "fleet.shutdown":
			reply(req.ID, map[string]any{"ok": true})
			return
		case "ext.throttle.get":
			cfg, err := loadConfig()
			if err != nil {
				replyErr(req.ID, err.Error())
				continue
			}
			reply(req.ID, cfg)
		case "ext.throttle.save":
			var cfg config
			if err := json.Unmarshal(req.Params, &cfg); err != nil {
				replyErr(req.ID, "invalid config")
				continue
			}
			if err := validateConfig(cfg); err != nil {
				replyErr(req.ID, err.Error())
				continue
			}
			if err := saveConfig(&cfg); err != nil {
				replyErr(req.ID, err.Error())
				continue
			}
			reply(req.ID, cfg)
		case "ext.throttle.sync":
			res, err := syncProfiles()
			if err != nil {
				replyErr(req.ID, err.Error())
				continue
			}
			reply(req.ID, res)
		case "ext.throttle.apply":
			var p struct {
				Name   string   `json:"name"`
				Hashes []string `json:"hashes"`
			}
			if err := json.Unmarshal(req.Params, &p); err != nil {
				replyErr(req.ID, "invalid params")
				continue
			}
			res, err := applyProfile(p.Name, p.Hashes)
			if err != nil {
				replyErr(req.ID, err.Error())
				continue
			}
			reply(req.ID, res)
		case "ext.throttle.global.get":
			raw, err := hostCall("fleet.throttle.get", map[string]any{})
			if err != nil {
				replyErr(req.ID, err.Error())
				continue
			}
			reply(req.ID, json.RawMessage(raw))
		case "ext.throttle.global.set":
			var p struct {
				DownRate int64 `json:"downRate"`
				UpRate   int64 `json:"upRate"`
			}
			if err := json.Unmarshal(req.Params, &p); err != nil {
				replyErr(req.ID, "invalid params")
				continue
			}
			raw, err := hostCall("fleet.throttle.set", map[string]any{"downRate": p.DownRate, "upRate": p.UpRate})
			if err != nil {
				replyErr(req.ID, err.Error())
				continue
			}
			reply(req.ID, json.RawMessage(raw))
		default:
			replyErr(req.ID, fmt.Sprintf("method not found: %s", req.Method))
		}
	}
}

func validateConfig(cfg config) error {
	seen := map[string]struct{}{}
	for i, p := range cfg.Profiles {
		name := strings.TrimSpace(p.Name)
		if name == "" {
			return fmt.Errorf("profiles[%d]: name required", i)
		}
		if strings.ContainsAny(name, " \t\r\n") {
			return fmt.Errorf("profiles[%d]: name must not contain whitespace", i)
		}
		if p.UpRate < 0 || p.DownRate < 0 {
			return fmt.Errorf("profiles[%d]: rates must be >= 0", i)
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("profiles[%d]: duplicate name %q", i, name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

func syncProfiles() (map[string]any, error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, err
	}
	synced := 0
	for _, p := range cfg.Profiles {
		if _, err := hostCall("fleet.throttle.define", map[string]any{
			"name":     strings.TrimSpace(p.Name),
			"upRate":   p.UpRate,
			"downRate": p.DownRate,
		}); err != nil {
			return nil, err
		}
		synced++
	}
	return map[string]any{"ok": true, "synced": synced}, nil
}

func applyProfile(name string, hashes []string) (map[string]any, error) {
	name = strings.TrimSpace(name)
	clean := make([]string, 0, len(hashes))
	for _, h := range hashes {
		h = strings.ToUpper(strings.TrimSpace(h))
		if h != "" {
			clean = append(clean, h)
		}
	}
	if len(clean) == 0 {
		return nil, fmt.Errorf("hashes required")
	}
	if name != "" {
		cfg, err := loadConfig()
		if err != nil {
			return nil, err
		}
		found := false
		for _, p := range cfg.Profiles {
			if strings.TrimSpace(p.Name) == name {
				if _, err := hostCall("fleet.throttle.define", map[string]any{
					"name": name, "upRate": p.UpRate, "downRate": p.DownRate,
				}); err != nil {
					return nil, err
				}
				found = true
				break
			}
		}
		if !found {
			// Allow applying a raw rTorrent throttle name without a saved profile.
			if _, err := hostCall("fleet.throttle.define", map[string]any{
				"name": name, "upRate": int64(0), "downRate": int64(0),
			}); err != nil {
				return nil, fmt.Errorf("define %q: %w", name, err)
			}
		}
	}
	raw, err := hostCall("fleet.throttle.assign", map[string]any{"hashes": clean, "name": name})
	if err != nil {
		return nil, err
	}
	var wrap map[string]any
	_ = json.Unmarshal(raw, &wrap)
	if wrap == nil {
		wrap = map[string]any{"ok": true}
	}
	wrap["hashes"] = clean
	wrap["name"] = name
	return wrap, nil
}

func loadConfig() (*config, error) {
	raw, err := hostCall("fleet.storage.get", map[string]any{"key": "profiles"})
	if err != nil {
		return nil, err
	}
	if string(raw) == "null" || len(raw) == 0 {
		return &config{Profiles: defaultProfiles()}, nil
	}
	var cfg config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return &config{Profiles: defaultProfiles()}, nil
	}
	if cfg.Profiles == nil {
		cfg.Profiles = []profile{}
	}
	return &cfg, nil
}

func saveConfig(cfg *config) error {
	_, err := hostCall("fleet.storage.put", map[string]any{"key": "profiles", "value": cfg})
	return err
}

func defaultProfiles() []profile {
	// Mirror ruTorrent's starter ladder (KiB/s → bytes/sec).
	out := make([]profile, 0, 8)
	for _, kib := range []int64{16, 32, 64, 128} {
		bps := kib * 1024
		out = append(out, profile{Name: fmt.Sprintf("thr_up%d", kib), Label: fmt.Sprintf("Up %d KiB/s", kib), UpRate: bps, DownRate: 0})
		out = append(out, profile{Name: fmt.Sprintf("thr_down%d", kib), Label: fmt.Sprintf("Down %d KiB/s", kib), UpRate: 0, DownRate: bps})
	}
	return out
}

func hostCall(method string, params any) (json.RawMessage, error) {
	ch := make(chan hostReply, 1)
	mu.Lock()
	id := nextHost
	nextHost++
	pending[id] = ch
	mu.Unlock()

	b, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
		"id":      id,
	})
	if err != nil {
		mu.Lock()
		delete(pending, id)
		mu.Unlock()
		return nil, err
	}
	outMu.Lock()
	fmt.Fprintf(os.Stdout, "%s\n", b)
	outMu.Unlock()

	reply := <-ch
	if reply.err != "" {
		return nil, fmt.Errorf("%s", reply.err)
	}
	return reply.result, nil
}

func reply(id any, result any) {
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "result": result, "id": id})
	outMu.Lock()
	fmt.Fprintf(os.Stdout, "%s\n", b)
	outMu.Unlock()
}

func replyErr(id any, msg string) {
	b, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"error":   map[string]any{"code": -32000, "message": msg},
		"id":      id,
	})
	outMu.Lock()
	fmt.Fprintf(os.Stdout, "%s\n", b)
	outMu.Unlock()
}

func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	default:
		return 0, false
	}
}
