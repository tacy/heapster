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
	"testing"
	"time"

	"github.com/open-falcon/common/model"
	"github.com/stretchr/testify/assert"
	"k8s.io/heapster/metrics/core"
	"sync"
)

type fakeOpenfalconProducer struct {
	msgs chan model.MetricValue
}

type fakeOpenfalconSink struct {
	core.DataSink
	fakeProducer *fakeOpenfalconProducer
}

type fakeSinkParam struct {
	servs []string
	maxgr int
	bs    int
	step  int
}

//NewFakeOpenfalconProducer fake openfalcon producer
func NewFakeOpenfalconProducer() *fakeOpenfalconProducer {
	return &fakeOpenfalconProducer{make(chan model.MetricValue, 100)}
}

func (producer *fakeOpenfalconProducer) Produce(servs []string, mb *[]model.MetricValue, wg *sync.WaitGroup, maxgr chan struct{}) {
	defer wg.Done()

	for _, msg := range *mb {
		producer.msgs <- msg
	}
}

// Returns a fake openfalcon sink.
func NewFakeSink(p fakeSinkParam) fakeOpenfalconSink {
	producer := NewFakeOpenfalconProducer()
	return fakeOpenfalconSink{
		&openfalconSink{
			producer:      producer,
			servs:         p.servs,
			step:          p.step,
			batchsize:     p.bs,
			maxconcurrent: p.maxgr,
		},
		producer,
	}
}

func TestStoreDataEmptyInput(t *testing.T) {
	fakeSink := NewFakeSink(fakeSinkParam{servs: []string{"192.178.1.1"}, step: 60, bs: 2, maxgr: 10})
	dataBatch := core.DataBatch{}
	fakeSink.ExportData(&dataBatch)
	result := []model.MetricValue{}
Loop:
	for {
		select {
		case i, ok := <-fakeSink.fakeProducer.msgs:
			if !ok {
				break Loop
			}
			result = append(result, i)

		case <-time.After(1 * time.Second):
			break Loop
		}
	}

	assert.Equal(t, 0, len(result))
}

func TestStoreMultipleDataInput(t *testing.T) {
	fakeSink := NewFakeSink(fakeSinkParam{servs: []string{"192.178.1.1", "192.184.3.1"}, step: 60, bs: 1, maxgr: 10})
	timestamp := time.Now()

	l := make(map[string]string)
	l["namespace_id"] = "123"
	l["container_name"] = "/system.slice/-.mount"
	l[core.LabelPodId.Key] = "aaaa-bbbb-cccc-dddd"

	l2 := make(map[string]string)
	l2["namespace_id"] = "123"
	l2["container_name"] = "/system.slice/dbus.service"
	l2[core.LabelPodId.Key] = "aaaa-bbbb-cccc-dddd"

	l3 := make(map[string]string)
	l3["namespace_id"] = "123"
	l3[core.LabelPodId.Key] = "aaaa-bbbb-cccc-dddd"

	l4 := make(map[string]string)
	l4[core.LabelPodNamespace.Key] = "123"
	l4["type"] = "node"
	l4[core.LabelPodName.Key] = "test-aaaa"
	l4[core.LabelPodId.Key] = "aaaa-bbbb-cccc-dddd"
	l4[core.LabelNodename.Key] = "origin"

	l5 := make(map[string]string)
	l5[core.LabelPodNamespace.Key] = "123"
	l5["type"] = "pod"
	l5[core.LabelPodName.Key] = "test-aaaa"
	l5[core.LabelPodId.Key] = "aaaa-bbbb-cccc-dddd"
	l5[core.LabelNodename.Key] = "origin"

	l6 := make(map[string]string)
	l6[core.LabelPodNamespace.Key] = "123"
	l6["type"] = "node"
	l6[core.LabelResourceID.Key] = "/dev/sda1"
	l6[core.LabelPodId.Key] = "aaaa-bbbb-cccc-dddd"
	l6[core.LabelNodename.Key] = "origin"

	metricSet1 := core.MetricSet{
		Labels: l,
		MetricValues: map[string]core.MetricValue{
			"/system.slice/-.mount//cpu/limit": {
				ValueType:  core.ValueInt64,
				MetricType: core.MetricCumulative,
				IntValue:   123456,
			},
		},
	}

	metricSet2 := core.MetricSet{
		Labels: l2,
		MetricValues: map[string]core.MetricValue{
			"/system.slice/dbus.service//cpu/usage": {
				ValueType:  core.ValueInt64,
				MetricType: core.MetricCumulative,
				IntValue:   123456,
			},
		},
	}

	metricSet3 := core.MetricSet{
		Labels: l3,
		MetricValues: map[string]core.MetricValue{
			"test/metric/1": {
				ValueType:  core.ValueInt64,
				MetricType: core.MetricCumulative,
				IntValue:   123456,
			},
		},
	}

	metricSet4 := core.MetricSet{
		Labels: l4,
		MetricValues: map[string]core.MetricValue{
			"cpu/node_utilization": {
				ValueType:  core.ValueInt64,
				MetricType: core.MetricCumulative,
				IntValue:   123456,
			},
		},
	}

	metricSet5 := core.MetricSet{
		Labels: l5,
		MetricValues: map[string]core.MetricValue{
			"network/tx_rate": {
				ValueType:  core.ValueInt64,
				MetricType: core.MetricCumulative,
				IntValue:   123456,
			},
		},
	}

	metricSet6 := core.MetricSet{
		Labels: l4,
		LabeledMetrics: []core.LabeledMetric{{Labels: l6,
			Name: "filesystem/usage",
			MetricValue: core.MetricValue{
				ValueType:  core.ValueInt64,
				MetricType: core.MetricCumulative,
				IntValue:   123456,
			}}},
		MetricValues: map[string]core.MetricValue{
			"cpu/node_utilization": {
				ValueType:  core.ValueInt64,
				MetricType: core.MetricCumulative,
				IntValue:   123456,
			},
		},
	}

	data := core.DataBatch{
		Timestamp: timestamp,
		MetricSets: map[string]*core.MetricSet{
			"pod1":  &metricSet1,
			"pod2":  &metricSet2,
			"pod3":  &metricSet3,
			"pod4":  &metricSet4,
			"pod5":  &metricSet5,
			"node1": &metricSet6,
		},
	}

	fakeSink.ExportData(&data)

	result := []model.MetricValue{}
Loop:
	for {
		select {
		case i, ok := <-fakeSink.fakeProducer.msgs:
			if !ok {
				break Loop
			}
			result = append(result, i)

		case <-time.After(1 * time.Second):
			break Loop
		}
	}

	assert.Equal(t, 4, len(result))
}
