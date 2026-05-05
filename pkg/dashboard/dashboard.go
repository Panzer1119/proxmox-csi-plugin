package dashboard

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/config"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

//go:embed static/*
var staticFS embed.FS

type Config struct {
	Enabled         bool
	BindAddress     string
	RefreshInterval time.Duration
	CloudConfigPath string
}

type Server struct {
	cfg                Config
	kube               kubernetes.Interface
	proxmox            *proxmoxpool.ProxmoxPool
	store              *Store
	httpServer         *http.Server
	collectMu          sync.Mutex
	intervalMu         sync.Mutex
	collectorCtx       context.Context
	collectorCancel    context.CancelFunc
	intervalCtx        context.Context
	intervalCancel     context.CancelFunc
	intervalRunning    bool
	topologyCollectMu  sync.Mutex
	topologyCollecting bool
}

func New(cfg Config, kube kubernetes.Interface) (*Server, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if cfg.RefreshInterval <= 0 {
		cfg.RefreshInterval = 15 * time.Second
	}
	if cfg.BindAddress == "" {
		cfg.BindAddress = ":8088"
	}
	cloud, err := config.ReadCloudConfigFromFile(cfg.CloudConfigPath)
	if err != nil {
		return nil, fmt.Errorf("read cloud config: %w", err)
	}
	pool, err := proxmoxpool.NewProxmoxPool(cloud.Clusters)
	if err != nil {
		return nil, fmt.Errorf("create proxmox pool: %w", err)
	}
	store := NewStore()
	srv := &Server{cfg: cfg, kube: kube, proxmox: pool, store: store}
	staticRoot, err := fs.Sub(staticFS, "static")
	if err != nil {
		return nil, fmt.Errorf("prepare dashboard static files: %w", err)
	}
	fileServer := http.FileServer(http.FS(staticRoot))
	mux := http.NewServeMux()
	mux.HandleFunc("/dashboard", srv.redirectDashboard)
	mux.HandleFunc("/dashboard/", srv.redirectDashboard)
	mux.Handle("/", fileServer)
	mux.HandleFunc("/api/topology", srv.handleTopology)
	mux.HandleFunc("/api/stream", srv.handleStream)
	srv.httpServer = &http.Server{Addr: cfg.BindAddress, Handler: mux}
	return srv, nil
}

func (s *Server) Start(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.collectorCtx, s.collectorCancel = context.WithCancel(context.Background())
	go func() {
		<-ctx.Done()
		s.stopIntervalCollector()
		if s.collectorCancel != nil {
			s.collectorCancel()
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.httpServer.Shutdown(shutdownCtx)
	}()
	klog.InfoS("Dashboard enabled", "address", s.cfg.BindAddress)
	if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) runCollector(ctx context.Context, collector *Collector) {
	ticker := time.NewTicker(s.cfg.RefreshInterval)
	defer ticker.Stop()
	defer klog.V(4).InfoS("Dashboard collector interval stopped")
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.collect(ctx, collector, "interval"); err != nil {
				klog.ErrorS(err, "Dashboard collector interval refresh failed")
			}
		}
	}
}

func (s *Server) collect(ctx context.Context, collector *Collector, reason string) error {
	s.collectMu.Lock()
	defer s.collectMu.Unlock()
	klog.V(4).InfoS("Dashboard collector running", "reason", reason)
	return collector.Collect(ctx, s.store)
}

func (s *Server) ensureIntervalCollectorRunning() {
	s.intervalMu.Lock()
	defer s.intervalMu.Unlock()
	if s.intervalRunning {
		return
	}
	s.intervalRunning = true
	ctx, cancel := context.WithCancel(s.collectorCtx)
	s.intervalCtx = ctx
	s.intervalCancel = cancel
	collector := &Collector{kube: s.kube, proxmox: s.proxmox}
	klog.V(4).InfoS("Dashboard collector interval started")
	if err := s.collect(ctx, collector, "stream-initial"); err != nil {
		klog.ErrorS(err, "Dashboard collector initial stream refresh failed")
	}
	go s.monitorCollector(ctx, collector)
}

func (s *Server) monitorCollector(ctx context.Context, collector *Collector) {
	s.runCollector(ctx, collector)
	s.intervalMu.Lock()
	s.intervalRunning = false
	s.intervalCtx = nil
	s.intervalCancel = nil
	s.intervalMu.Unlock()
}

func (s *Server) stopIntervalCollector() {
	s.intervalMu.Lock()
	if !s.intervalRunning {
		s.intervalMu.Unlock()
		return
	}
	cancel := s.intervalCancel
	s.intervalCancel = nil
	s.intervalCtx = nil
	s.intervalRunning = false
	s.intervalMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *Server) redirectDashboard(w http.ResponseWriter, r *http.Request) {
	target := strings.TrimPrefix(r.URL.Path, "/dashboard")
	if target == "" {
		target = "/"
	}
	http.Redirect(w, r, target, http.StatusMovedPermanently)
}

func (s *Server) handleTopology(w http.ResponseWriter, r *http.Request) {
	const topologyTTL = 5 * time.Second
	// Check if store has valid cached snapshot
	if snapshot := s.store.GetIfValid(topologyTTL); snapshot.Regions != nil {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(snapshot)
		return
	}
	// Ensure only one concurrent collection
	s.topologyCollectMu.Lock()
	if s.topologyCollecting {
		// Wait for the ongoing collection
		s.topologyCollectMu.Unlock()
		// Spin until collection is done and cache is valid
		for {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(10 * time.Millisecond):
				if snapshot := s.store.GetIfValid(topologyTTL); snapshot.Regions != nil {
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(snapshot)
					return
				}
			}
		}
	}
	s.topologyCollecting = true
	s.topologyCollectMu.Unlock()
	defer func() {
		s.topologyCollectMu.Lock()
		s.topologyCollecting = false
		s.topologyCollectMu.Unlock()
	}()
	// Collect fresh topology
	collector := &Collector{kube: s.kube, proxmox: s.proxmox}
	if err := s.collect(r.Context(), collector, "topology"); err != nil {
		klog.ErrorS(err, "Dashboard topology refresh failed")
		http.Error(w, "topology refresh failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.store.Get())
}
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "stream unsupported", http.StatusInternalServerError)
		return
	}
	ch, cancel := s.store.Subscribe()
	s.ensureIntervalCollectorRunning()
	defer func() {
		cancel()
		if s.store.SubscriberCount() == 0 {
			s.stopIntervalCollector()
		}
	}()
	for {
		select {
		case <-r.Context().Done():
			return
		case snapshot := <-ch:
			buf, _ := json.Marshal(snapshot)
			_, _ = fmt.Fprintf(w, "data: %s\n\n", buf)
			flusher.Flush()
		}
	}
}

type Store struct {
	mu          sync.RWMutex
	snapshot    Snapshot
	subscribers map[int]chan Snapshot
	nextID      int
}

func NewStore() *Store { return &Store{subscribers: map[int]chan Snapshot{}} }
func (s *Store) Set(ss Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot = ss
	for _, ch := range s.subscribers {
		select {
		case ch <- ss:
		default:
		}
	}
}
func (s *Store) Get() Snapshot { s.mu.RLock(); defer s.mu.RUnlock(); return s.snapshot }
func (s *Store) IsValid(ttl time.Duration) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.snapshot.Regions == nil {
		return false
	}
	return time.Since(s.snapshot.GeneratedAt) < ttl
}
func (s *Store) GetIfValid(ttl time.Duration) Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.snapshot.Regions == nil || time.Since(s.snapshot.GeneratedAt) >= ttl {
		return Snapshot{}
	}
	return s.snapshot
}
func (s *Store) SubscriberCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.subscribers)
}
func (s *Store) Subscribe() (<-chan Snapshot, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextID
	s.nextID++
	ch := make(chan Snapshot, 4)
	s.subscribers[id] = ch
	return ch, func() { s.mu.Lock(); defer s.mu.Unlock(); delete(s.subscribers, id); close(ch) }
}
