package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func main() {
	// TODO: Make query request to Mimir
	// TODO: Send immediately canceled query request to Mimir
	// TODO: Send non-canceled query request to Mimir
	// TODO: Verify success
	start := time.Date(2023, 10, 23, 13, 30, 0, 0, time.UTC)
	end := time.Date(2023, 10, 23, 13, 45, 0, 0, time.UTC)
	fmt.Printf("Start: %d, end: %d\n", start.Unix(), end.Unix())

	writeSeries()

	/*
		for i := 0; i < 10; i++ {
			cancelingRequests()
		}

		for i := 0; i < 10; i++ {
			normalRequests()
		}
	*/
}

func writeSeries() {
	const u = "http://localhost:8000/api/v1/push"

	var timeseries []mimirpb.PreallocTimeseries
	timeseries = append(timeseries, mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels: []mimirpb.LabelAdapter{
				{Name: "sampleLabel1", Value: "sampleValue1"},
				{Name: "sampleLabel2", Value: "sampleValue2"},
			},
			Samples: []mimirpb.Sample{
				{Value: 1, TimestampMs: 2},
				{Value: 3, TimestampMs: 4},
			},
		},
	})

	wr := mimirpb.WriteRequest{
		Timeseries: timeseries,
		Source:     mimirpb.API,
		Metadata: []*mimirpb.MetricMetadata{
			{
				Type: mimirpb.COUNTER,
				Help: "Testing",
				Unit: "?",
			},
		},
	}
	// TODO: Encode message with protobuf and compress with Snappy
	data, err := proto.Marshal(&wr)
	if err != nil {
		panic(err)
	}

	enc := snappy.Encode(nil, data)
	r := bytes.NewReader(enc)

	// ^(.*)((?i)foo|foobar)(.*)$
	// But instead of foo|foobar replace it with a large number of alternations, like 500 alternations (so aaaa|bbbb|cccc|… go on for 500 times).
	ctx := context.Background()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, r)
	req.Header.Add("X-Prometheus-Remote-Write-Version", "0.1.0")
	if err != nil {
		panic(err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Request failed: %s\n", body)
	} else {
		fmt.Printf("Request succeeded\n")
	}
}

func cancelingRequests() {
	const u = "http://localhost:8007/prometheus/api/v1/query?query=cortex_ring_members"

	// TODO: Use the following:
	// ^(.*)((?i)foo|foobar)(.*)$
	// But instead of foo|foobar replace it with a large number of alternations, like 500 alternations (so aaaa|bbbb|cccc|… go on for 500 times).
	// Use an instant query

	timeout := 20 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		panic(err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	if resp.StatusCode/100 != 2 {
		fmt.Printf("Request failed\n")
	} else {
		fmt.Printf("Request succeeded\n")
	}
}

func normalRequests() {
}
