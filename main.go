package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "config.yaml"
	}
	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("[fatal] load config: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, err := NewDB(ctx, cfg.Database.DSN)
	if err != nil {
		log.Fatalf("[fatal] db: %v", err)
	}
	defer db.Close()

	auth := NewAuth(cfg.Sub2API.BaseURL)
	subClient := NewSub2APIClient(cfg.Sub2API.BaseURL, cfg.Sub2API.AdminAPIKey, db)
	laneMonitor := NewLaneBoardMonitor(db, subClient)

	// 泳道图监控器
	monCtx, monCancel := context.WithCancel(context.Background())
	defer monCancel()
	laneMonitor.Start(monCtx)

	srv := NewServer(db, cfg, auth, laneMonitor)
	httpSrv := &http.Server{
		Addr:              net.JoinHostPort(cfg.Server.Host, cfg.Server.Port),
		Handler:           srv.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	go func() {
		log.Printf("[http] listening on %s", httpSrv.Addr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[fatal] http: %v", err)
		}
	}()

	// graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Printf("[main] shutting down...")
	shCtx, shCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer shCancel()
	if err := httpSrv.Shutdown(shCtx); err != nil {
		log.Printf("[warn] http shutdown: %v", err)
		_ = httpSrv.Close()
	}
	monCancel()
	laneMonitor.Wait()
}
