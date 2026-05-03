package dashboard

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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
	cfg        Config
	kube       kubernetes.Interface
	proxmox    *proxmoxpool.ProxmoxPool
	store      *Store
	httpServer *http.Server
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
	mux := http.NewServeMux()
	mux.Handle("/dashboard/", http.StripPrefix("/dashboard", http.FileServer(http.FS(staticFS))))
	mux.HandleFunc("/dashboard/api/topology", srv.handleTopology)
	mux.HandleFunc("/dashboard/api/stream", srv.handleStream)
	srv.httpServer = &http.Server{Addr: cfg.BindAddress, Handler: mux}
	return srv, nil
}

func (s *Server) Start(ctx context.Context) error {
	if s == nil {
		return nil
	}
	collector := &Collector{kube: s.kube, proxmox: s.proxmox}
	_ = collector.Collect(ctx, s.store)
	go s.runCollector(ctx, collector)
	go func() {
		<-ctx.Done()
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
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = collector.Collect(ctx, s.store)
		}
	}
}
func (s *Server) handleTopology(w http.ResponseWriter, _ *http.Request) {
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
	defer cancel()
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
func (s *Store) Subscribe() (<-chan Snapshot, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextID
	s.nextID++
	ch := make(chan Snapshot, 4)
	s.subscribers[id] = ch
	return ch, func() { s.mu.Lock(); defer s.mu.Unlock(); delete(s.subscribers, id); close(ch) }
}
