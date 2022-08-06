package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"mos/storage/engine"
	"mos/storage/server"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-contrib/pprof"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	port = flag.Int("port", 8080, "http listening port")
	dir  = flag.String("dir", "", "storage root directory")
)

var endpointPrefix = "/storage_node/"

var etcdCfg = clientv3.Config{
	Endpoints: []string{
		"http://localhost:2379",
		"http://localhost:22379",
		"http://localhost:32379",
	},
	DialTimeout:          time.Second * 30,
	DialKeepAliveTimeout: time.Second * 30,
}

func main() {
	flag.Parse()
	config := engine.DefaultConfig()
	if *dir != "" {
		config.RootDirectory = *dir
	}
	s, err := server.NewServer(config)
	if err != nil {
		panic(err)
	}
	defer s.Close()
	router := s.SetRouter()
	pprof.Register(router)
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: router,
	}
	go func() {
		err := srv.ListenAndServe()
		if err != nil {
			log.Println(err)
		}
	}()
	go ServiceRegistry(*port)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	sig := <-sigCh
	log.Println(fmt.Sprintf("Got signal [%s] to exit.", sig))
	// The context is used to inform the server it has 5 seconds to finish
	// the request it is currently handling
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown: ", err)
	}
	log.Println("Server shutdown")
}

func ServiceRegistry(port int) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Println(err)
		return
	}
	var endpoint string
	for _, address := range addrs {
		// 检查ip地址判断是否回环地址
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				endpoint = fmt.Sprintf("%s:%d", ipnet.IP.String(), port)
				break
			}
		}
	}
	cli, err := clientv3.New(etcdCfg)
	if err != nil {
		panic(err)
	}
	key := endpointPrefix + endpoint
	ctx := context.Background()
	ttl := 3
	// 创建租约
	lease, err := cli.Grant(ctx, int64(ttl))
	if err != nil {
		panic(err)
	}
	b, _ := json.Marshal(lease)
	log.Printf("grant lease suucess: %s\n", string(b))
	// 通过租约上报endpoint
	res, err := cli.Put(ctx, key, endpoint, clientv3.WithLease(lease.ID))
	if err != nil {
		panic(err)
	}
	b, _ = json.Marshal(res)
	log.Printf("put kv with lease suucess: %s\n", string(b))
	// 保持租约不过期
	klRes, err := cli.KeepAlive(ctx, lease.ID)
	if err != nil {
		panic(err)
	}
	// 监听续约情况
	for v := range klRes {
		b, _ = json.Marshal(v)
		fmt.Printf("keep lease alive suucess: %s\n", string(b))
	}
	log.Println("stop keeping lease alive")
}
