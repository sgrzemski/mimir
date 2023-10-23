package main

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

func main() {
	// TODO: Make query request to Mimir
	// TODO: Send immediately canceled query request to Mimir
	// TODO: Send non-canceled query request to Mimir
	// TODO: Verify success
	start := time.Date(2023, 10, 23, 13, 30, 0, 0, time.UTC)
	end := time.Date(2023, 10, 23, 13, 45, 0, 0, time.UTC)
	fmt.Printf("Start: %d, end: %d\n", start.Unix(), end.Unix())

	for i := 0; i < 10; i++ {
		cancelingRequests()
	}

	for i := 0; i < 10; i++ {
		normalRequests()
	}
}

func cancelingRequests() {
	const u = "http://localhost:8007/prometheus/api/v1/query?query=cortex_ring_members"

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
