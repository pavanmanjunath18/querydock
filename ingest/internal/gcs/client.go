// Package gcs is a small, deliberately polite anonymous client for public GCS
// buckets: a global request-rate limit, a concurrency cap, exponential backoff
// with jitter on 429/5xx/network errors (honouring Retry-After), and an adaptive
// slowdown whenever the server pushes back.
package gcs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

const (
	apiBase   = "https://storage.googleapis.com/storage/v1/b/"
	userAgent = "querydock-collector/0.1 (+https://github.com/pavanmanjunath18/querydock)"
)

// ErrNotFound is returned for 4xx responses other than 429 (Kettle treats these as "absent").
var ErrNotFound = errors.New("gcs: object not found")

// Options controls politeness. Zero values get safe defaults.
type Options struct {
	RPS         float64       // sustained requests per second across all workers (default 20)
	Concurrency int           // max in-flight requests (default 16)
	MaxRetries  int           // per request (default 8)
	MaxBackoff  time.Duration // cap per sleep (default 60s)
	BaseURL     string        // for tests
}

// Stats are cumulative counters, safe to read at any time.
type Stats struct {
	Requests  atomic.Int64
	Retries   atomic.Int64
	Throttled atomic.Int64 // 429/503 responses
	NotFound  atomic.Int64
	WireBytes atomic.Int64 // bytes read from the network (TLS + HTTP), i.e. download size
	BodyBytes atomic.Int64 // decoded response bytes
	SlowDowns atomic.Int64
}

type Client struct {
	http    *http.Client
	lim     *rate.Limiter
	sem     chan struct{}
	opts    Options
	base    string
	Stats   Stats
	mu      sync.Mutex
	slowTil time.Time
}

type countingConn struct {
	net.Conn
	n *atomic.Int64
}

func (c countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.n.Add(int64(n))
	return n, err
}

func New(o Options) *Client {
	if o.RPS <= 0 {
		o.RPS = 20
	}
	if o.Concurrency <= 0 {
		o.Concurrency = 16
	}
	if o.MaxRetries <= 0 {
		o.MaxRetries = 8
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = 60 * time.Second
	}
	c := &Client{opts: o, base: apiBase, sem: make(chan struct{}, o.Concurrency)}
	if o.BaseURL != "" {
		c.base = o.BaseURL
	}
	c.lim = rate.NewLimiter(rate.Limit(o.RPS), int(o.RPS)+1)
	dialer := &net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return countingConn{conn, &c.Stats.WireBytes}, nil
		},
		MaxIdleConnsPerHost: o.Concurrency,
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   true,
	}
	c.http = &http.Client{Transport: tr, Timeout: 120 * time.Second}
	return c
}

// slowDown halves the request rate for a minute after the server pushes back.
func (c *Client) slowDown() {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur := float64(c.lim.Limit())
	next := cur / 2
	if next < 1 {
		next = 1
	}
	c.lim.SetLimit(rate.Limit(next))
	c.slowTil = time.Now().Add(time.Minute)
	c.Stats.SlowDowns.Add(1)
}

func (c *Client) maybeRecover() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cur := float64(c.lim.Limit()); cur < c.opts.RPS && time.Now().After(c.slowTil) {
		next := cur * 1.25
		if next > c.opts.RPS {
			next = c.opts.RPS
		}
		c.lim.SetLimit(rate.Limit(next))
		c.slowTil = time.Now().Add(15 * time.Second)
	}
}

func (c *Client) backoff(attempt int, retryAfter string) time.Duration {
	if s, err := strconv.Atoi(retryAfter); err == nil && s > 0 {
		return min(time.Duration(s)*time.Second, c.opts.MaxBackoff)
	}
	d := time.Duration(float64(500*time.Millisecond) * float64(int(1)<<min(attempt, 10)))
	d = min(d, c.opts.MaxBackoff)
	return time.Duration(rand.Float64() * float64(d)) // full jitter
}

// do performs a GET with rate limiting, the concurrency cap and retries.
func (c *Client) do(ctx context.Context, u string) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt <= c.opts.MaxRetries; attempt++ {
		if attempt > 0 {
			c.Stats.Retries.Add(1)
		}
		c.maybeRecover()
		if err := c.lim.Wait(ctx); err != nil {
			return nil, err
		}
		select {
		case c.sem <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		body, status, retryAfter, err := c.once(ctx, u)
		<-c.sem
		switch {
		case err == nil && status == http.StatusOK:
			return body, nil
		case err == nil && status >= 400 && status < 500 && status != http.StatusTooManyRequests:
			c.Stats.NotFound.Add(1)
			return nil, ErrNotFound
		case err == nil && (status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable):
			c.Stats.Throttled.Add(1)
			c.slowDown()
			lastErr = fmt.Errorf("gcs: HTTP %d", status)
		case err == nil:
			lastErr = fmt.Errorf("gcs: HTTP %d", status)
		default:
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			lastErr = err
		}
		select {
		case <-time.After(c.backoff(attempt, retryAfter)):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("gcs: giving up on %s: %w", u, lastErr)
}

func (c *Client) once(ctx context.Context, u string) ([]byte, int, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, 0, "", err
	}
	req.Header.Set("User-Agent", userAgent)
	c.Stats.Requests.Add(1)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	c.Stats.BodyBytes.Add(int64(len(body)))
	if err != nil {
		return nil, 0, "", err
	}
	return body, resp.StatusCode, resp.Header.Get("Retry-After"), nil
}

// Get downloads an object. Missing objects return ErrNotFound.
func (c *Client) Get(ctx context.Context, bucket, object string) ([]byte, error) {
	return c.do(ctx, c.base+bucket+"/o/"+url.PathEscape(object)+"?alt=media")
}

// ListOptions narrows a listing. StartOffset/EndOffset are lexicographic bounds on
// object names (inclusive/exclusive), which lets us list only one day of builds.
type ListOptions struct {
	Prefix      string
	Delimiter   string
	StartOffset string
	EndOffset   string
}

// List returns child prefixes ("directories") and object names under a prefix.
func (c *Client) List(ctx context.Context, bucket string, o ListOptions) (prefixes, names []string, err error) {
	q := url.Values{}
	q.Set("prefix", o.Prefix)
	q.Set("maxResults", "1000")
	q.Set("fields", "nextPageToken,prefixes,items(name)")
	if o.Delimiter != "" {
		q.Set("delimiter", o.Delimiter)
	}
	if o.StartOffset != "" {
		q.Set("startOffset", o.StartOffset)
	}
	if o.EndOffset != "" {
		q.Set("endOffset", o.EndOffset)
	}
	for {
		body, err := c.do(ctx, c.base+bucket+"/o?"+q.Encode())
		if errors.Is(err, ErrNotFound) {
			return prefixes, names, nil
		}
		if err != nil {
			return nil, nil, err
		}
		var page struct {
			NextPageToken string   `json:"nextPageToken"`
			Prefixes      []string `json:"prefixes"`
			Items         []struct {
				Name string `json:"name"`
			} `json:"items"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, nil, fmt.Errorf("gcs: decoding listing: %w", err)
		}
		prefixes = append(prefixes, page.Prefixes...)
		for _, it := range page.Items {
			names = append(names, it.Name)
		}
		if page.NextPageToken == "" {
			return prefixes, names, nil
		}
		q.Set("pageToken", page.NextPageToken)
	}
}
