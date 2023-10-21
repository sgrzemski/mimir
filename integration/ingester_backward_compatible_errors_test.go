// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/ingester_limits_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker

package integration

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

func TestTenantSeriesLimitErrors(t *testing.T) {
	maxErrorsBeforeStop := 100
	additionalFlags := []string{"-ingester.max-global-series-per-user", "1000", "-ingester.max-global-series-per-metric", "300"}
	for _, grpcErrorsEnabled := range []bool{false, true} {
		testName := fmt.Sprintf("tesing tenant limits with ingester.grpc-errors-enabled=%v", grpcErrorsEnabled)
		t.Run(testName, func(t *testing.T) {
			s, c := setupTest(t, grpcErrorsEnabled, additionalFlags...)
			defer s.Close()

			now := time.Now()
			expectedHTTPStatus := 400

			// Try to push as many series with the different metric name as we can.
			seriesName := "test_limit_per_metric"
			expectedResponseFormat := "failed pushing to ingester: user=e2e-user: per-metric series limit of 300 exceeded (err-mimir-max-series-per-metric). To adjust the related per-tenant limit, configure -ingester.max-global-series-per-metric, or contact your service administrator. This is for series {__name__=\"%s\", cardinality=\"%d\"}\n"
			for i, errs := 0, 0; i < 10000; i++ {
				cardinality := rand.Int()
				series, _, _ := generateAlternatingSeries(i)(seriesName, now, prompb.Label{
					Name:  "cardinality",
					Value: strconv.Itoa(cardinality),
				})

				expectedResponse := fmt.Sprintf(expectedResponseFormat, seriesName, cardinality)
				res, body, err := c.PushWithBody(series)
				require.NoError(t, err)

				if res.StatusCode != 200 {
					require.Equal(t, expectedHTTPStatus, res.StatusCode)
					require.Equal(t, expectedResponse, string(body))
					if errs++; errs >= maxErrorsBeforeStop {
						break
					}
				}
			}

			// Try to push as many series with the different metric name as we can.
			expectedResponse := "failed pushing to ingester: user=e2e-user: per-user series limit of 1000 exceeded (err-mimir-max-series-per-user). To adjust the related per-tenant limit, configure -ingester.max-global-series-per-user, or contact your service administrator.\n"
			for i, errs := 0, 0; i < 10000; i++ {
				series, _, _ := generateAlternatingSeries(i)(fmt.Sprintf("test_limit_per_tenant_%d", rand.Int()), now)
				res, body, err := c.PushWithBody(series)
				require.NoError(t, err)

				if res.StatusCode != 200 {
					require.Equal(t, expectedHTTPStatus, res.StatusCode)
					require.Equal(t, expectedResponse, string(body))
					if errs++; errs >= maxErrorsBeforeStop {
						break
					}
				}
			}
		})
	}
}

func TestTenantMetadataLimitErrors(t *testing.T) {
	maxErrorsBeforeStop := 100
	additionalFlags := []string{"-ingester.max-global-metadata-per-metric", "1"}
	for _, grpcErrorsEnabled := range []bool{false, true} {
		testName := fmt.Sprintf("tesing tenant limits with ingester.grpc-errors-enabled=%v", grpcErrorsEnabled)
		t.Run(testName, func(t *testing.T) {
			s, c := setupTest(t, grpcErrorsEnabled, additionalFlags...)
			defer s.Close()

			now := time.Now()
			expectedHTTPStatus := 400

			// Try to push as many series with the different metric name as we can.
			seriesName := "test_limit_per_metric"
			expectedResponseFormat := "failed pushing to ingester: user=e2e-user: per-metric series limit of 300 exceeded (err-mimir-max-series-per-metric). To adjust the related per-tenant limit, configure -ingester.max-global-series-per-metric, or contact your service administrator. This is for series {__name__=\"%s\", cardinality=\"%d\"}\n"
			for i, errs := 0, 0; i < 10000; i++ {
				cardinality := rand.Int()
				label := fmt.Sprintf("cardinality%d", i)
				series, _, _ := generateAlternatingSeries(i)(seriesName, now, prompb.Label{
					Name:  label,
					Value: strconv.Itoa(cardinality),
				})

				expectedResponse := fmt.Sprintf(expectedResponseFormat, seriesName, cardinality)
				res, body, err := c.PushWithBody(series)
				require.NoError(t, err)

				if res.StatusCode != 200 {
					require.Equal(t, expectedHTTPStatus, res.StatusCode)
					require.Equal(t, expectedResponse, string(body))
					if errs++; errs >= maxErrorsBeforeStop {
						break
					}
				}
			}

			// Try to push as many series with the different metric name as we can.
			expectedResponse := "failed pushing to ingester: user=e2e-user: per-user series limit of 1000 exceeded (err-mimir-max-series-per-user). To adjust the related per-tenant limit, configure -ingester.max-global-series-per-user, or contact your service administrator.\n"
			for i, errs := 0, 0; i < 10000; i++ {
				series, _, _ := generateAlternatingSeries(i)(fmt.Sprintf("test_limit_per_tenant_%d", rand.Int()), now)
				res, body, err := c.PushWithBody(series)
				require.NoError(t, err)

				if res.StatusCode != 200 {
					require.Equal(t, expectedHTTPStatus, res.StatusCode)
					require.Equal(t, expectedResponse, string(body))
					if errs++; errs >= maxErrorsBeforeStop {
						break
					}
				}
			}
		})
	}
}

func TestTenantLimitErrors1(t *testing.T) {
	//maxErrorsBeforeStop := 100
	additionalFlags := []string{"-ingester.max-global-series-per-user", "1000", "-ingester.max-global-series-per-metric", "300"}
	testCases := map[string]struct {
		expectedHTTPStatus     int
		expectedResponseFormat string
		testFn                 func(*testing.T, *e2emimir.Client, string, int, int)
	}{
		"err-mimir-max-series-per-user": {
			expectedHTTPStatus:     400,
			expectedResponseFormat: "failed pushing to ingester: user=e2e-user: per-user series limit of 300 exceeded (err-mimir-max-series-per-user). To adjust the related per-tenant limit, configure -ingester.max-global-series-per-user, or contact your service administrator.\n",
			testFn:                 maxSeriesPerUserTest(),
		},
		"err-mimir-max-series-per-metric": {
			expectedHTTPStatus:     400,
			expectedResponseFormat: "failed pushing to ingester: user=e2e-user: per-metric series limit of 300 exceeded (err-mimir-max-series-per-metric). To adjust the related per-tenant limit, configure -ingester.max-global-series-per-metric, or contact your service administrator. This is for series {__name__=\"%s\", cardinality=\"%d\"}\n",
			testFn:                 maxSeriesPerMetricTest(),
		},
	}

	for _, grpcErrorsEnabled := range []bool{false, true} {
		for testID, testData := range testCases {
			s, c := setupTest(t, grpcErrorsEnabled, additionalFlags...)
			defer s.Close()
			testName := fmt.Sprintf("tesing %s with ingester.grpc-errors-enabled=%v", testID, grpcErrorsEnabled)
			t.Run(testName, func(t *testing.T) {
				testData.testFn(t, c, testData.expectedResponseFormat, testData.expectedHTTPStatus, math.MaxInt)
			})
		}
	}
}

func maxSeriesPerUserTest() func(*testing.T, *e2emimir.Client, string, int, int) {
	return func(t *testing.T, c *e2emimir.Client, expectedResponseFormat string, expectedHTTPStatus, maxErrorsBeforeStop int) {
		now := time.Now()
		for i, errs := 0, 0; i < 10000; i++ {
			series, _, _ := generateAlternatingSeries(i)(fmt.Sprintf("test_limit_per_tenant_%d", rand.Int()), now)
			res, body, err := c.PushWithBody(series)
			require.NoError(t, err)

			if res.StatusCode != 200 {
				require.Equal(t, expectedHTTPStatus, res.StatusCode)
				require.Equal(t, expectedResponseFormat, string(body))
				if errs++; errs >= maxErrorsBeforeStop {
					break
				}
			}
		}
	}
}

func maxSeriesPerMetricTest() func(*testing.T, *e2emimir.Client, string, int, int) {
	return func(t *testing.T, c *e2emimir.Client, expectedResponseFormat string, expectedHTTPStatus, maxErrorsBeforeStop int) {
		seriesName := "test_limit_per_metric"
		now := time.Now()
		for i, errs := 0, 0; i < 10000; i++ {
			cardinality := rand.Int()
			series, _, _ := generateAlternatingSeries(i)(seriesName, now, prompb.Label{
				Name:  "cardinality",
				Value: strconv.Itoa(cardinality),
			})

			expectedResponse := fmt.Sprintf(expectedResponseFormat, seriesName, cardinality)
			res, body, err := c.PushWithBody(series)
			require.NoError(t, err)

			if res.StatusCode != 200 {
				require.Equal(t, expectedHTTPStatus, res.StatusCode)
				require.Equal(t, expectedResponse, string(body))
				if errs++; errs >= maxErrorsBeforeStop {
					break
				}
			}
		}
	}
}

func setupTest(t *testing.T, grpcErrorsEnabled bool, additionalFlags ...string) (*e2e.Scenario, *e2emimir.Client) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
	)
	flags["-ingester.ring.replication-factor"] = "1"
	flags["-ingester.ring.heartbeat-period"] = "1s"
	flags["-ingester.grpc-errors-enabled"] = strconv.FormatBool(grpcErrorsEnabled)
	for i := 0; i < len(additionalFlags)/2; i++ {
		flags[additionalFlags[2*i]] = additionalFlags[2*i+1]
	}

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Mimir components.
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	ingester1 := e2emimir.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), flags)
	ingester2 := e2emimir.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), flags)
	ingester3 := e2emimir.NewIngester("ingester-3", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, ingester1, ingester2, ingester3))

	// Wait until distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// Wait until ingesters have heartbeated the ring after all ingesters were active,
	// in order to update the number of instances. Since we have no metric, we have to
	// rely on a ugly sleep.
	time.Sleep(2 * time.Second)

	client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", userID)
	require.NoError(t, err)
	return s, client
}
