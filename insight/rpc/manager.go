package rpc

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/prometheus/prometheus/storage"

	"auto-monitor/common/log"
	"auto-monitor/common/rpc"
	"auto-monitor/common/rpc/gen-go/metrics"
)

type Appendable interface {
	Appender() (storage.Appender, error)
}

type Manager struct {
	sync.Mutex
	stopped           bool
	workerPool        *WorkerPool
	rpcServer         *rpc.MetricsRpcServer
	rpcSender         *rpc.SendManager
	whiteList         map[string]bool
	whiteListSwitcher bool
}

//addr:local rpc server addr, for listen
//remoteAddr: remote rpc server addr, for send
func NewManager(addr, remoteAddr, datasource string, appender Appendable, whiteListFile string, whiteListSwitcher bool) (*Manager, error) {
	rpcServer := rpc.NewMetricsRpcServer(addr)
	var rpcClient *rpc.SendManager
	if len(remoteAddr) > 8 {
		rpcClient = rpc.NewSendManager("remote", remoteAddr)
		rpcClient.Datasource = datasource
		log.Infof("remote rpc server is: %s", remoteAddr)
		log.Infof("datasource is: %s", datasource)
	}

	c := runtime.NumCPU()
	if c > 16 {
		c = c - 6
	}
	pool := NewWorkerPool(c, appender)

	m := &Manager{
		stopped:    true,
		rpcServer:  rpcServer,
		rpcSender:  rpcClient,
		workerPool: pool,
	}
	//获取preset metrics list
	var err error
	mMap := make(map[string]bool)
	if whiteListSwitcher {
		type Yaml struct {
			MetricList []string `yaml:"metric_list,omitempty"`
		}

		if _, err = os.Stat(whiteListFile); err != nil {
			log.Infof("whiteListFile doesn't exist, err: %s", err.Error())
			goto QUIT
		}

		b, err := ioutil.ReadFile(whiteListFile)
		if err != nil {
			log.Infof("load whiteListFile err: %s", err.Error())
			goto QUIT
		}

		conf := new(Yaml)
		if err = json.Unmarshal(b, conf); err != nil {
			log.Infof("Unmarshal whiteListFile err: %s", err.Error())
			goto QUIT
		}

		for _, m := range conf.MetricList {
			m = strings.Trim(m, " ")
			mMap[m] = true
		}
	}

QUIT:
	m.whiteListSwitcher = whiteListSwitcher
	m.whiteList = mMap
	pool.manager = m
	return m, err
}

func (m *Manager) Start() error {
	if m.stopped {
		m.stopped = false

		if m.rpcSender != nil {
			m.rpcSender.Run()
		}

		m.workerPool.Run()

		handler := rpc.MetricsTransferHandler{
			Processor:               m.workerPool.Write,
			ProcessorWithDatasource: m.workerPool.WriteWithDatasource,
		}
		if err := m.rpcServer.Run(handler); err != nil {
			log.Errorf("run rpc server error:%s", err.Error())
		}
	}
	return nil
}

func (m *Manager) Stop() {
	log.Info("rpc manager will stop")

	m.Lock()
	defer m.Unlock()

	if !m.stopped {
		m.stopped = true
		if m.rpcServer != nil {
			m.rpcServer.Stop()
		}
		if m.workerPool != nil {
			m.workerPool.Stop()
		}

		if m.rpcSender != nil {
			m.rpcSender.Stop()
		}
	}

	log.Info("rpc manager stop!!!")
}

func (m *Manager) WriteToRemote(ms *metrics.Metrics) {
	if m.rpcSender != nil {
		mList := ms
		if m.whiteListSwitcher {
			mList = m.getFilteredMetrics(ms)
		}
		if err := m.rpcSender.Send(mList); err != nil {
			log.Errorf("write to remote error:%s", err.Error())
		}
	}
}

func (m *Manager) getFilteredMetrics(ms *metrics.Metrics) *metrics.Metrics {
	newMs := &metrics.Metrics{List: make([]*metrics.Metric, 0)}
	for _, metric := range ms.List {
		if m.checkIsInWhiteList(metric) {
			newMs.List = append(newMs.List, metric)
		}
	}
	return newMs
}

func (m *Manager) checkIsInWhiteList(metric *metrics.Metric) bool {
	mKey := metric.MetricKey
	index := strings.Index(mKey, "{")
	if index >= 0 {
		mKey = mKey[:index]
	}
	if _, in := m.whiteList[mKey]; in {
		return true
	} else {
		return false
	}
}

func (m *Manager) SendRemote() bool {
	return m.rpcSender != nil
}
