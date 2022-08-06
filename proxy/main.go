package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/gin-gonic/gin"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type member string

func (m member) String() string {
	return string(m)
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

var endpointPrefix = "/storage_node/"

var endpoints []consistent.Member

var owners = make(map[int]string)

// 全局服务锁
var serviceLocker = sync.RWMutex{}

// etcdCfg Etcd配置
var etcdCfg = clientv3.Config{
	Endpoints: []string{
		"http://localhost:2379",
		"http://localhost:22379",
		"http://localhost:32379",
	},
	DialTimeout:          time.Second * 30,
	DialKeepAliveTimeout: time.Second * 30,
}

var consistentConfig = consistent.Config{
	Hasher:            hasher{},
	PartitionCount:    65535,
	ReplicationFactor: 20,
	Load:              1.25,
}

func main() {
	client, err := clientv3.New(etcdCfg)
	if err != nil {
		panic(err)
	}
	c, err := StartUp(client)
	if err != nil {
		panic(err)
	}
	httpClient := &http.Client{}
	go func() {
		DetectClusterChange(client, c, httpClient)
	}()
	router := SetRouter(c, httpClient)
	srv := http.Server{
		Addr:    ":6666",
		Handler: router,
	}
	go func() {
		err := srv.ListenAndServe()
		if err != nil {
			log.Fatal(err)
		}
	}()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	sig := <-sigCh
	fmt.Println(sig)
}

func StartUp(client *clientv3.Client) (*consistent.Consistent, error) {
	ctx := context.Background()
	resp, err := client.Get(ctx, endpointPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	serviceLocker.Lock()
	defer serviceLocker.Unlock()
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		endpoint := strings.TrimPrefix(key, endpointPrefix)
		endpoints = append(endpoints, member(endpoint))
	}
	c := consistent.New(endpoints, consistentConfig)
	for partID := 0; partID < consistentConfig.PartitionCount; partID++ {
		owners[partID] = c.GetPartitionOwner(partID).String()
	}
	return c, nil
}

func DetectClusterChange(client *clientv3.Client, c *consistent.Consistent, httpClient *http.Client) {
	ctx := context.Background()
	ch := client.Watch(ctx, endpointPrefix, clientv3.WithPrefix(), clientv3.WithPrevKV())
	for item := range ch {
		for _, event := range item.Events {
			key := string(event.Kv.Key)
			endpoint := strings.TrimPrefix(key, endpointPrefix)
			serviceLocker.Lock()
			switch event.Type {
			case clientv3.EventTypePut:
				c.Add(member(endpoint))
			case clientv3.EventTypeDelete:
				c.Remove(endpoint)
			}
			serviceLocker.Unlock()
		}
	}
}

func SetRouter(c *consistent.Consistent, httpClient *http.Client) http.Handler {
	router := gin.New()
	putObjectHandler := func(ctx *gin.Context) {
		serviceLocker.Lock()
		defer serviceLocker.Unlock()
		objectname := ctx.Param("objectname")
		if objectname == "" {
			ctx.String(http.StatusBadRequest, "empty object name")
			return
		}
		username := ctx.GetHeader("x-mos-username")
		if username == "" {
			ctx.String(http.StatusBadRequest, "empty user name")
			return
		}
		value, err := io.ReadAll(ctx.Request.Body)
		if err != nil {
			ctx.String(http.StatusInternalServerError, "read object content error: %s", err.Error())
			return
		}
		key := []byte(fmt.Sprintf("%s_%s", username, objectname))
		endpoint := c.LocateKey(key).String()
		req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s/%s", endpoint, objectname), bytes.NewReader(value))
		if err != nil {
			ctx.String(http.StatusInternalServerError, "construct req error: %s", err.Error())
			return
		}
		req.Header.Set("x-mos-username", username)
		resp, err := httpClient.Do(req)
		if err != nil {
			ctx.String(http.StatusInternalServerError, "send request error: %s", err.Error())
			return
		}
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			ctx.String(http.StatusInternalServerError, "read response error", err.Error())
			return
		}
		ctx.String(resp.StatusCode, string(data))
	}
	getObjectHandler := func(ctx *gin.Context) {
		serviceLocker.RLock()
		defer serviceLocker.RUnlock()
		objectname := ctx.Param("objectname")
		if objectname == "" {
			ctx.String(http.StatusBadRequest, "empty object name")
			return
		}
		username := ctx.GetHeader("x-mos-username")
		if username == "" {
			ctx.String(http.StatusBadRequest, "empty user name")
			return
		}
		key := []byte(fmt.Sprintf("%s_%s", username, objectname))
		endpoint := c.LocateKey(key).String()
		req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/%s", endpoint, objectname), nil)
		if err != nil {
			ctx.String(http.StatusInternalServerError, "construct req error: %s", err.Error())
			return
		}
		req.Header.Set("x-mos-username", username)
		resp, err := httpClient.Do(req)
		if err != nil {
			ctx.String(http.StatusInternalServerError, "send request error: %s", err.Error())
			return
		}
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			ctx.String(http.StatusInternalServerError, "read response error", err.Error())
			return
		}
		ctx.Data(resp.StatusCode, "application/octet-stream", data)
	}
	deleteObjectHandler := func(ctx *gin.Context) {
		serviceLocker.Lock()
		defer serviceLocker.Unlock()
		objectname := ctx.Param("objectname")
		if objectname == "" {
			ctx.String(http.StatusBadRequest, "empty object name")
			return
		}
		username := ctx.GetHeader("x-mos-username")
		if username == "" {
			ctx.String(http.StatusBadRequest, "empty user name")
			return
		}
		key := []byte(fmt.Sprintf("%s_%s", username, objectname))
		endpoint := c.LocateKey(key).String()
		req, err := http.NewRequest("DELETE", fmt.Sprintf("http://%s/%s", endpoint, objectname), nil)
		if err != nil {
			ctx.String(http.StatusInternalServerError, "construct req error: %s", err.Error())
			return
		}
		req.Header.Set("x-mos-username", username)
		resp, err := httpClient.Do(req)
		if err != nil {
			ctx.String(http.StatusInternalServerError, "send request error: %s", err.Error())
			return
		}
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			ctx.String(http.StatusInternalServerError, "read response error", err.Error())
			return
		}
		ctx.String(resp.StatusCode, string(data))
	}
	router.PUT("/:objectname", putObjectHandler)
	router.GET("/:objectname", getObjectHandler)
	router.DELETE("/:objectname", deleteObjectHandler)
	return router
}
