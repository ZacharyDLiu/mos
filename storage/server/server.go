package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"mos/storage/engine"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
)

const preallocate = 70000

type Stats struct {
	KeyCount int64 `json:"key_count"`
	Space    int64 `json:"space"`
}

type Server struct {
	Engine *engine.MKV
}

func NewServer(config *engine.Config, options ...engine.Option) (*Server, error) {
	e, err := engine.Open(config, options...)
	if err != nil {
		return nil, err
	}
	return &Server{
		Engine: e,
	}, nil
}

func (s *Server) SetRouter() *gin.Engine {
	//router := gin.Default()
	router := gin.New()
	router.PUT("/:objectname", s.putObjectHandler)
	router.GET("/:objectname", s.getObjectHandler)
	router.DELETE("/:objectname", s.deleteObjectHandler)

	router.GET("/stats", s.getStatsHandler)

	router.PUT("/exp/:objectname", s.putObjectHandlerV2)
	return router
}

func (s *Server) putObjectHandler(ctx *gin.Context) {
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
	err = s.Engine.Put(key, value)
	if err != nil {
		ctx.String(http.StatusInternalServerError, "store object err: %s", err.Error())
		return
	}
	ctx.String(http.StatusOK, "object have been stored")
	return
}

func (s *Server) putObjectHandlerV2(ctx *gin.Context) {
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
	key := fmt.Sprintf("%s_%s", username, objectname)
	data, err := formData(ctx, key)
	if err != nil {
		ctx.String(http.StatusInternalServerError, "form data error: %s", err.Error())
		return
	}
	if err := s.Engine.PutData(data, key); err != nil {
		ctx.String(http.StatusInternalServerError, "store object err: %s", err.Error())
		return
	}
	ctx.String(http.StatusOK, "object have been stored")
	return
}

func formData(ctx *gin.Context, key string) ([]byte, error) {
	buf := make([]byte, 0, preallocate)
	buffer := bytes.NewBuffer(buf)
	buffer.WriteByte(engine.NormalFlag)
	if err := binary.Write(buffer, binary.BigEndian, uint16(len(key))); err != nil {
		return nil, err
	}
	vsize := make([]byte, 4)
	buffer.Write(vsize)
	buffer.WriteString(key)
	n, err := buffer.ReadFrom(ctx.Request.Body)
	if err != nil {
		return nil, err
	}
	data := buffer.Bytes()
	binary.BigEndian.PutUint32(data[3:7], uint32(n))
	checksum := crc32.ChecksumIEEE(data)
	if err := binary.Write(buffer, binary.BigEndian, checksum); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (s *Server) getObjectHandler(ctx *gin.Context) {
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
	value, err := s.Engine.Get(key)
	if err != nil {
		if err == engine.ErrKeyNotFound {
			ctx.String(http.StatusNotFound, "object not found")
			return
		}
		ctx.String(http.StatusInternalServerError, "get object error: %s", err.Error())
		return
	}
	ctx.Data(http.StatusOK, "application/octet-stream", value)
	return
}

func (s *Server) deleteObjectHandler(ctx *gin.Context) {
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
	err := s.Engine.Delete(key)
	if err != nil {
		ctx.String(http.StatusInternalServerError, "delete object error: %s", err.Error())
		return
	}
	ctx.String(http.StatusOK, "object have been deleted")
	return
}

func (s *Server) getStatsHandler(ctx *gin.Context) {
	user2stats := make(map[string]*Stats)
	f := func(key string, entry *engine.Entry) error {
		username, _, found := strings.Cut(key, "_")
		if !found {
			return errors.New("invalid key")
		}
		stats := user2stats[username]
		if stats == nil {
			stats = new(Stats)
		}
		stats.KeyCount += 1
		stats.Space += int64(entry.Size)
		user2stats[username] = stats
		return nil
	}
	err := s.Engine.Walk(f)
	if err != nil {
		ctx.String(http.StatusInternalServerError, "get stats error: %s", err.Error())
		return
	}
	ctx.JSON(http.StatusOK, user2stats)
	return
}

func (s *Server) Close() error {
	return s.Engine.Close()
}
