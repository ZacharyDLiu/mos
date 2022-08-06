package server

import (
	"bytes"
	"fmt"
	"io"
	"mos/storage/engine"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerBasicOperations(t *testing.T) {
	config := engine.DefaultConfig()
	err := os.RemoveAll(config.RootDirectory)
	require.Nil(t, err)

	s, err := NewServer(nil)
	require.Nil(t, err)
	defer s.Close()

	router := s.SetRouter()
	username := "admin"
	expected := []byte(fmt.Sprintf("%065536d", 123))
	for i := 0; i < 100000; i++ {
		objectname := fmt.Sprintf("test_%d", i)
		{
			body := bytes.NewReader(expected)
			req, err := http.NewRequest("PUT", fmt.Sprintf("http://localhost:8080/%s", objectname), body)
			require.Nil(t, err)
			req.Header.Set("x-mos-username", username)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, req)
			assert.Equal(t, http.StatusOK, recorder.Code)
		}

		{
			req, err := http.NewRequest("GET", fmt.Sprintf("http://localhost:8080/%s", objectname), nil)
			assert.Nil(t, err)
			req.Header.Set("x-mos-username", username)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, req)
			assert.Equal(t, http.StatusOK, recorder.Code)
			res := recorder.Result()
			actual, err := io.ReadAll(res.Body)
			assert.Nil(t, err)
			assert.Equal(t, expected, actual)
		}

		{
			req, err := http.NewRequest("DELETE", fmt.Sprintf("http://localhost:8080/%s", objectname), nil)
			assert.Nil(t, err)
			req.Header.Set("x-mos-username", username)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, req)
			assert.Equal(t, http.StatusOK, recorder.Code)
		}

		{
			req, err := http.NewRequest("GET", fmt.Sprintf("http://localhost:8080/%s", objectname), nil)
			assert.Nil(t, err)
			req.Header.Set("x-mos-username", username)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, req)
			assert.Equal(t, http.StatusNotFound, recorder.Code)
		}
	}
}

func TestStats(t *testing.T) {
	config := engine.DefaultConfig()
	err := os.RemoveAll(config.RootDirectory)
	require.Nil(t, err)

	s, err := NewServer(nil)
	require.Nil(t, err)

	router := s.SetRouter()
	usernames := []string{"a", "b", "c", "d", "e"}
	expected := []byte(fmt.Sprintf("%065536d", 123))
	for i := 0; i < 1000; i++ {
		objectname := fmt.Sprintf("test_%d", i)
		body := bytes.NewReader(expected)
		req, err := http.NewRequest("PUT", fmt.Sprintf("http://localhost:8080/%s", objectname), body)
		require.Nil(t, err)
		req.Header.Set("x-mos-username", usernames[i%5])
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, req)
		assert.Equal(t, http.StatusOK, recorder.Code)
	}
	err = s.Close()
	require.Nil(t, err)
}

func TestBasicOperation(t *testing.T) {
	config := engine.DefaultConfig()
	err := os.RemoveAll(config.RootDirectory)
	require.Nil(t, err)

	s, err := NewServer(nil)
	require.Nil(t, err)
	defer s.Close()

	router := s.SetRouter()
	username := "default"
	dir := "/home/liuzichen/tmp/data/"
	for i := 1; i <= 100000; i++ {
		objectname := fmt.Sprintf("test_%d", i)
		file, err := os.Open(dir + objectname)
		require.Nil(t, err)
		expected, err := io.ReadAll(file)
		require.Nil(t, err)
		err = file.Close()
		require.Nil(t, err)

		{
			req, err := http.NewRequest("PUT", fmt.Sprintf("http://localhost:8080/%s", objectname), bytes.NewReader(expected))
			require.Nil(t, err)
			req.Header.Set("x-mos-username", username)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, req)
			assert.Equal(t, http.StatusOK, recorder.Code)
		}

		{
			req, err := http.NewRequest("GET", fmt.Sprintf("http://localhost:8080/%s", objectname), nil)
			require.Nil(t, err)
			req.Header.Set("x-mos-username", username)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, req)
			assert.Equal(t, http.StatusOK, recorder.Code)
			actual, err := io.ReadAll(recorder.Result().Body)
			require.Nil(t, err)
			require.Equal(t, expected, actual)
		}
	}
}

func TestPutV2(t *testing.T) {
	config := engine.DefaultConfig()
	err := os.RemoveAll(config.RootDirectory)
	require.Nil(t, err)

	s, err := NewServer(nil)
	require.Nil(t, err)
	defer s.Close()

	router := s.SetRouter()
	username := "default"
	dir := "/home/liuzichen/tmp/data/"
	for i := 1; i <= 1; i++ {
		objectname := fmt.Sprintf("test_%d", i)
		file, err := os.Open(dir + objectname)
		require.Nil(t, err)
		expected, err := io.ReadAll(file)
		require.Nil(t, err)
		err = file.Close()
		require.Nil(t, err)

		{
			req, err := http.NewRequest("PUT", fmt.Sprintf("http://localhost:8080/exp/%s", objectname), bytes.NewReader(expected))
			require.Nil(t, err)
			req.Header.Set("x-mos-username", username)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, req)
			assert.Equal(t, http.StatusOK, recorder.Code)
		}

		{
			req, err := http.NewRequest("GET", fmt.Sprintf("http://localhost:8080/%s", objectname), nil)
			require.Nil(t, err)
			req.Header.Set("x-mos-username", username)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, req)
			assert.Equal(t, http.StatusOK, recorder.Code)
			actual, err := io.ReadAll(recorder.Result().Body)
			require.Nil(t, err)
			require.Equal(t, expected, actual)
		}
	}
}

func TestPutObjectToServer(t *testing.T) {
	client := &http.Client{
		Transport: &http.Transport{
			MaxConnsPerHost: 1000,
		},
	}
	total := 100000
	n := 100
	batch := total / n
	dir := "/home/liuzichen/tmp/data/"
	username := "default"
	putObject := func(i int) {
		objectname := fmt.Sprintf("test_%d", i)
		file, err := os.Open(dir + objectname)
		require.Nil(t, err)
		defer file.Close()
		req, err := http.NewRequest("PUT", fmt.Sprintf("http://localhost:8080/%s", objectname), file)
		require.Nil(t, err)
		req.Header.Set("x-mos-username", username)
		resp, err := client.Do(req)
		defer resp.Body.Close()
		require.Nil(t, err)

		// validate object has been successfully put
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := i*batch + 1; j <= (i+1)*batch; j++ {
				putObject(j)
			}
		}(i)
	}
	wg.Wait()
	fmt.Println("put 100000 64KiB objects to server successfully, it takes:", time.Since(start))
}

func TestGetObjectFromServer(t *testing.T) {
	client := &http.Client{
		Transport: &http.Transport{
			MaxConnsPerHost: 1000,
		},
	}
	n := 100
	batch := 100000 / n
	dir := "/home/liuzichen/tmp/data/"
	username := "default"
	getObject := func(i int) {
		objectname := fmt.Sprintf("test_%d", i)
		file, err := os.Open(dir + objectname)
		require.Nil(t, err)
		expected, err := io.ReadAll(file)
		require.Nil(t, err)
		// validate the object content is indeed 64KiB
		require.Equal(t, 65536, len(expected))
		err = file.Close()
		require.Nil(t, err)

		req, err := http.NewRequest("GET", fmt.Sprintf("http://localhost:8080/%s", objectname), nil)
		require.Nil(t, err)
		req.Header.Set("x-mos-username", username)
		resp, err := client.Do(req)
		defer resp.Body.Close()
		require.Nil(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		actual, err := io.ReadAll(resp.Body)
		require.Nil(t, err)
		// validate object content is the same
		require.Equal(t, expected, actual)
	}
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := i*batch + 1; j <= (i+1)*batch; j++ {
				getObject(j)
			}
		}(i)
	}
	wg.Wait()
	fmt.Println("getting 100000 64KiB objects from server and validating successfully, it takes:", time.Since(start))
}
