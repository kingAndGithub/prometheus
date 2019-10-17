// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remote

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
        "strconv"
         
        "github.com/garyburd/redigo/redis"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/version"

	"github.com/prometheus/prometheus/prompb"
)

const maxErrMsgLen = 256

var userAgent = fmt.Sprintf("Prometheus/%s", version.Version)

// Client allows reading and writing from/to a remote HTTP endpoint.
type Client struct {
	index   int // Used to differentiate clients in metrics.
	url     *config_util.URL
	client  *http.Client
	timeout time.Duration
        redisaddr        string
	rediskey         string
	redisswitcher    bool
	redisreplica     string
}

// ClientConfig configures a Client.
type ClientConfig struct {
	URL              *config_util.URL
	Timeout          model.Duration
	HTTPClientConfig config_util.HTTPClientConfig
        RedisAddr        string
	RedisKey         string
	RedisSwitcher    bool
	RedisReplica     string
}

// NewClient creates a new Client.
func NewClient(index int, conf *ClientConfig) (*Client, error) {
	httpClient, err := config_util.NewClientFromConfig(conf.HTTPClientConfig, "remote_storage", false)
	if err != nil {
		return nil, err
	}

	return &Client{
		index:   index,
		url:     conf.URL,
		client:  httpClient,
		timeout: time.Duration(conf.Timeout),
                redisaddr:conf.RedisAddr,
		rediskey:conf.RedisKey,
		redisswitcher:conf.RedisSwitcher,
                redisreplica:conf.RedisReplica,
	}, nil
}

type recoverableError struct {
	error
}

func (c *Client) JudgeMaster() bool {
	if c.redisaddr == "" || c.redisswitcher == false || c.rediskey == ""{
		return true
	}

	if  c.redisswitcher == true {
		con, err := redis.Dial("tcp", c.redisaddr)
		if err != nil {
                        fmt.Printf("redis.Dial %v \n", err.Error())
			return true
		}
		defer con.Close()
		_, err = con.Do("SELECT", 0)
		if err != nil {
			return true
		}
                fmt.Printf("SELECT \n")
		rep, err:=strconv.Atoi(c.redisreplica)
		if err != nil {
			return true
		}
                fmt.Printf("strconv.Atoi rep %v \n", rep)
		res, err := con.Do("SETNX", c.rediskey, rep)
		if res != int64(1) {
                        if err != nil {
                             fmt.Printf("setnx %v \n", err.Error())
                        }
			replica, err := redis.String(con.Do("GET", c.rediskey))
			if err != nil {
                             fmt.Printf("GET %v \n", err.Error())
		             return true
			}
                        fmt.Printf("replica %v\n", replica)
			if replica == c.redisreplica {
				_, err = con.Do("EXPIRE", c.rediskey, 600)
                                if err != nil {
                                    fmt.Printf("expire %v \n", err.Error())
                                }
				return true
			}else {
				return false
			}
		}else{
                        _, err = con.Do("EXPIRE", c.rediskey, 600)
                        if err != nil {
                            fmt.Printf("else expire %v \n", err.Error())
                        }
			return true
		}
	}
	return false
}

// Store sends a batch of samples to the HTTP endpoint, the request is the proto marshalled
// and encoded bytes from codec.go.
func (c *Client) Store(ctx context.Context, req []byte) error {
        ok := c.JudgeMaster()
	if !ok {
		return nil
	}
	httpReq, err := http.NewRequest("POST", c.url.String(), bytes.NewReader(req))
	if err != nil {
		// Errors from NewRequest are from unparseable URLs, so are not
		// recoverable.
		return err
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", userAgent)
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	httpReq = httpReq.WithContext(ctx)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	httpResp, err := c.client.Do(httpReq.WithContext(ctx))
	if err != nil {
		// Errors from client.Do are from (for example) network errors, so are
		// recoverable.
		return recoverableError{err}
	}
	defer func() {
		io.Copy(ioutil.Discard, httpResp.Body)
		httpResp.Body.Close()
	}()

	if httpResp.StatusCode/100 != 2 {
		scanner := bufio.NewScanner(io.LimitReader(httpResp.Body, maxErrMsgLen))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		err = errors.Errorf("server returned HTTP status %s: %s", httpResp.Status, line)
	}
	if httpResp.StatusCode/100 == 5 {
		return recoverableError{err}
	}
	return err
}

// Name identifies the client.
func (c Client) Name() string {
	return fmt.Sprintf("%d:%s", c.index, c.url)
}

// Read reads from a remote endpoint.
func (c *Client) Read(ctx context.Context, query *prompb.Query) (*prompb.QueryResult, error) {
	req := &prompb.ReadRequest{
		// TODO: Support batching multiple queries into one read request,
		// as the protobuf interface allows for it.
		Queries: []*prompb.Query{
			query,
		},
	}
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to marshal read request")
	}

	compressed := snappy.Encode(nil, data)
	httpReq, err := http.NewRequest("POST", c.url.String(), bytes.NewReader(compressed))
	if err != nil {
		return nil, errors.Wrap(err, "unable to create request")
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Add("Accept-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", userAgent)
	httpReq.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	httpResp, err := c.client.Do(httpReq.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "error sending request")
	}
	defer func() {
		io.Copy(ioutil.Discard, httpResp.Body)
		httpResp.Body.Close()
	}()

	compressed, err = ioutil.ReadAll(httpResp.Body)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("error reading response. HTTP status code: %s", httpResp.Status))
	}

	if httpResp.StatusCode/100 != 2 {
		return nil, errors.Errorf("remote server %s returned HTTP status %s: %s", c.url.String(), httpResp.Status, strings.TrimSpace(string(compressed)))
	}

	uncompressed, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, errors.Wrap(err, "error reading response")
	}

	var resp prompb.ReadResponse
	err = proto.Unmarshal(uncompressed, &resp)
	if err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal response body")
	}

	if len(resp.Results) != len(req.Queries) {
		return nil, errors.Errorf("responses: want %d, got %d", len(req.Queries), len(resp.Results))
	}

	return resp.Results[0], nil
}
