// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package openfalconsink

import (
	"fmt"
	"math/rand"
	"net"
	"net/rpc/jsonrpc"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/open-falcon/common/model"
	"k8s.io/heapster/metrics/core"
)

//Producer interface
type Producer interface {
	Produce(servs []string, mb *[]model.MetricValue, wg *sync.WaitGroup, maxgr chan struct{})
}

type openfalconProducer struct {
}

type openfalconSink struct {
	producer      Producer //open-falcon produce
	servs         []string //open-falcon transfer server
	step          int      //open-falcon metric step
	batchsize     int      //push metric batch size
	maxconcurrent int      //push metric max concurrent goroutine
}

// type opmetric struct {
// 	Metric      string      `json:"metric"`
// 	Endpoint    string      `json:"endpoint"`
// 	Tags        string      `json:"tags"`
// 	Value       interface{} `json:"value"`
// 	Timestamp   int64       `json:"timestamp"`
// 	Step        int         `json:"step"`
// 	CounterType string      `json:"counterType"`
// }

//TransferResponse is openfalcon rpc server response
// type TransferResponse struct {
// 	Message string
// 	Total   int
// 	Invalid int
// 	Latency int64
// }

// func (tr *TransferResponse) String() string {
// 	return fmt.Sprintf(
// 		"<Total=%v, Invalid:%v, Latency=%vms, Message:%s>",
// 		tr.Total,
// 		tr.Invalid,
// 		tr.Latency,
// 		tr.Message,
// 	)
// }

func (p *openfalconProducer) Produce(servs []string, mb *[]model.MetricValue, wg *sync.WaitGroup, concurrentGoroutines chan struct{}) {
	defer func() {
		concurrentGoroutines <- struct{}{}
		wg.Done()
	}()

	<-concurrentGoroutines

	// b, err := json.Marshal(rs)
	// if err != nil {
	// 	glog.Warningf("json parse err: %v", err)
	// 	return
	// }
	rand.Seed(time.Now().UnixNano())
	for _, i := range servs {
		conn, err := net.DialTimeout("tcp", i, time.Duration(10*time.Second))
		if err != nil {
			glog.Warningf("dial %s fail: %v", i, err)
			continue
		}
		rpcC := jsonrpc.NewClient(conn)
		var resp model.TransferResponse
		err = rpcC.Call("Transfer.Update", mb, &resp)
		if err != nil {
			glog.Errorf("Export metric to %s fail: %v ", i, err)
			break
		}
		glog.Infof("Export metric to Openfalcon: %s", resp.String())
	}
}

func (o *openfalconSink) Name() string {
	return "Openfalcon Sink"
}

func (o *openfalconSink) Stop() {
	// Do nothing.
}

func batchToOpmetric(dataBatch *core.DataBatch, batchsize int, channel chan *[]model.MetricValue) {
	var opms = []model.MetricValue{}

	mbatch := func(opms *[]model.MetricValue, channel chan *[]model.MetricValue) {
		if len(*opms) >= batchsize {
			rb := make([]model.MetricValue, len(*opms))
			copy(rb, *opms)
			channel <- &rb
			*opms = nil
		}
	}

	defer close(channel)

	for _, metricSet := range dataBatch.MetricSets {
		mt := metricSet.Labels["type"]
		if mt == "pod" || mt == "node" {
			var lb string
			var ep string
			for metricName, metricValue := range metricSet.MetricValues {
				var ct string

				switch metricName {
				case "cpu/limit", "memory/limit", "cpu/usage_rate", "memory/usage", "cpu/node_utilization", "memory/node_utilization", "network/rx_rate", "network/tx_rate":
					ct = "GAUGE"
				//case "network/rx", "network/tx":
				//	ct = "COUNTER"
				default:
					continue
				}

				ml := metricSet.Labels
				if mt == "node" {
					ep = ml["nodename"]
					lb = fmt.Sprintf("nodename=%s", ml["nodename"])
				} else {
					pn := ml["pod_name"]
					ep = pn[:strings.LastIndex(pn, "-")]
					lb = fmt.Sprintf("pod_name=%s,pod_id=%s,pod_namespace=%s,nodename=%s, app=%s",
						ml["pod_name"], ml["pod_id"], ml["pod_namespace"], ml["nodename"], ep)
				}

				point := model.MetricValue{
					Metric:    strings.Replace(metricName, "/", ".", -1),
					Endpoint:  ep,
					Tags:      lb,
					Value:     metricValue.GetValue(),
					Timestamp: dataBatch.Timestamp.Local().Unix(),
					Step:      60,
					Type:      ct,
				}

				opms = append(opms, point)
				mbatch(&opms, channel)

			}

			for _, metric := range metricSet.LabeledMetrics {
				var ct string
				switch metric.Name {
				case "filesystem/usage", "filesystem/limit":
					ct = "GAUGE"
				default:
					continue
				}

				lb = fmt.Sprintf("%s,resource_id=%s", lb, metric.Labels["resource_id"])
				point := model.MetricValue{
					Metric:    strings.Replace(metric.Name, "/", ".", -1),
					Endpoint:  ep,
					Tags:      lb,
					Value:     metric.MetricValue.GetValue(),
					Timestamp: dataBatch.Timestamp.Local().Unix(),
					Step:      60,
					Type:      ct,
				}
				opms = append(opms, point)
				mbatch(&opms, channel)
			}
		}
	}

	if len(opms) >= 1 {
		channel <- &opms
	}

}

func (o *openfalconSink) ExportData(batch *core.DataBatch) {
	channel := make(chan *[]model.MetricValue, o.maxconcurrent)
	wg := &sync.WaitGroup{}

	concurrentGoroutines := make(chan struct{}, o.maxconcurrent)

	for i := 0; i < o.maxconcurrent; i++ {
		concurrentGoroutines <- struct{}{}
	}

	go batchToOpmetric(batch, o.batchsize, channel)

Loop:
	for {
		select {
		case rs, ok := <-channel:
			if !ok {
				break Loop
			}
			wg.Add(1)
			go o.producer.Produce(o.servs, rs, wg, concurrentGoroutines)

		case <-time.After(30 * time.Second):
			glog.Warningln("Export metric to Openfalcon timeout!")
			return
		}
	}

	wg.Wait()
}

//CreateOpenfalconSink create openfalcon sink
func CreateOpenfalconSink(uri *url.URL) (core.DataSink, error) {
	opts, err := url.ParseQuery(uri.RawQuery)
	step := 60
	batchsize := 1000
	maxconcurrent := 5
	if err != nil {
		return nil, fmt.Errorf("failed to parser url's query string: %s", err)
	}

	if _, ok := opts["server"]; !ok {
		return nil, fmt.Errorf("failed get param server: %s", uri)
	}

	if v, ok := opts["step"]; ok {
		if step, err = strconv.Atoi(v[0]); err != nil {
			return nil, fmt.Errorf("failed to parser param step: %s", err)
		}
	}

	if v, ok := opts["batchsize"]; ok {
		if batchsize, err = strconv.Atoi(v[0]); err != nil {
			return nil, fmt.Errorf("failed to parser param batchsize: %s", err)
		}
	}

	if v, ok := opts["maxconcurrent"]; ok {
		if maxconcurrent, err = strconv.Atoi(v[0]); err != nil {
			return nil, fmt.Errorf("failed to parser param maxconcurrent: %s", err)
		}
	}

	return &openfalconSink{producer: &openfalconProducer{}, servs: opts["server"], step: step, batchsize: batchsize, maxconcurrent: maxconcurrent}, nil
}
