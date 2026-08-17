package main

import (
	"context"
	"log"
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
	subClient := NewSub2APIClient(cfg.Sub2API.BaseURL, cfg.Sub2API.AdminAPIKey)
	rdb := NewRedisClient(cfg.Redis.Addr, cfg.Redis.Password)
	if err := rdb.Ping(ctx); err != nil {
		log.Printf("[warn] redis ping: %v (sched:acc 缓存将无法清理)", err)
	}
	laneMonitor := NewLaneBoardMonitor(db, subClient, rdb)

	// 泳道图监控器
	monCtx, monCancel := context.WithCancel(context.Background())
	defer monCancel()
	go laneMonitor.Start(monCtx)

	srv := NewServer(db, cfg, auth, laneMonitor)
	httpSrv := &http.Server{
		Addr:    ":" + cfg.Server.Port,
		Handler: srv.Routes(),
	}

	go func() {
		log.Printf("[http] listening on :%s", cfg.Server.Port)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[fatal] http: %v", err)
		}
	}()

	// graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Printf("[main] shutting down...")
	shCtx, shCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shCancel()
	_ = httpSrv.Shutdown(shCtx)
}
