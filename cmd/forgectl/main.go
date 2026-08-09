// forgectl is the operator's CLI. A thin HTTP client over the same public
// API everything else uses -- no privileged back channel, so anything
// forgectl can do, any authenticated client can do, and the API surface
// stays the single source of capability.
//
// Usage:
//
//	forgectl [-api URL] [-token T] <command> [args]
//
//	stats                        job counts + latency percentiles (last hour)
//	depths                       live queue depths
//	workers                      registered workers (DB view, includes gone)
//	live                         connected workers (control-plane view)
//	job <id>                     one job, fully
//	attempts <id>                a job's attempt history
//	cancel <id>                  cancel a job
//	dlq                          dead-lettered jobs
//	requeue <id>                 send a dead-lettered job back to work
//	drain <worker>               graceful shutdown, now
//	pause <worker>               stop fetching, stay alive
//	resume <worker>              resume fetching
//	concurrency <worker> <n>     live-resize a worker's pool
//
// FORGE_API_URL and FORGE_API_TOKEN are the flag defaults.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	api := flag.String("api", envOr("FORGE_API_URL", "http://127.0.0.1:8080"), "forge-api base URL")
	token := flag.String("token", os.Getenv("FORGE_API_TOKEN"), "bearer token")
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}
	c := client{base: *api, token: *token}

	var err error
	switch cmd := args[0]; cmd {
	case "stats":
		since := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
		err = c.get("/api/v1/stats/summary?since=" + since)
	case "depths":
		err = c.get("/api/v1/queues/depths")
	case "workers":
		err = c.get("/api/v1/workers")
	case "live":
		err = c.get("/api/v1/control/workers")
	case "job":
		err = c.withArg(args, 1, func(id string) error { return c.get("/api/v1/jobs/" + id) })
	case "attempts":
		err = c.withArg(args, 1, func(id string) error { return c.get("/api/v1/jobs/" + id + "/attempts") })
	case "cancel":
		err = c.withArg(args, 1, func(id string) error { return c.do("DELETE", "/api/v1/jobs/"+id, nil) })
	case "dlq":
		err = c.get("/api/v1/dlq")
	case "requeue":
		err = c.withArg(args, 1, func(id string) error { return c.do("POST", "/api/v1/dlq/"+id+"/requeue", nil) })
	case "drain":
		err = c.withArg(args, 1, func(w string) error { return c.do("POST", "/api/v1/control/workers/"+w+"/drain", nil) })
	case "pause":
		err = c.withArg(args, 1, func(w string) error { return c.do("POST", "/api/v1/control/workers/"+w+"/pause", nil) })
	case "resume":
		err = c.withArg(args, 1, func(w string) error { return c.do("POST", "/api/v1/control/workers/"+w+"/resume", nil) })
	case "concurrency":
		if len(args) < 3 {
			err = fmt.Errorf("usage: forgectl concurrency <worker> <target>")
			break
		}
		body := fmt.Sprintf(`{"target":%s}`, args[2])
		err = c.do("POST", "/api/v1/control/workers/"+args[1]+"/concurrency", []byte(body))
	default:
		err = fmt.Errorf("unknown command %q", cmd)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "forgectl:", err)
		os.Exit(1)
	}
}

type client struct {
	base, token string
}

func (c client) withArg(args []string, n int, f func(string) error) error {
	if len(args) <= n {
		return fmt.Errorf("missing argument")
	}
	return f(args[n])
}

func (c client) get(path string) error { return c.do("GET", path, nil) }

func (c client) do(method, path string, body []byte) error {
	req, err := http.NewRequest(method, c.base+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	// Pretty-print JSON; pass through anything else untouched.
	var pretty bytes.Buffer
	if json.Indent(&pretty, raw, "", "  ") == nil {
		fmt.Println(pretty.String())
	} else {
		os.Stdout.Write(raw)
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("%s %s -> %d", method, path, resp.StatusCode)
	}
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func usage() {
	fmt.Fprintf(os.Stderr, `forgectl -- operate a Forge deployment

usage: forgectl [-api URL] [-token T] <command> [args]

  stats | depths | workers | live | dlq
  job <id> | attempts <id> | cancel <id> | requeue <id>
  drain <worker> | pause <worker> | resume <worker> | concurrency <worker> <n>

env: FORGE_API_URL, FORGE_API_TOKEN
`)
}
