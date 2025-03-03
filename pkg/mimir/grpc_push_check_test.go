// SPDX-License-Identifier: AGPL-3.0-only

package mimir

import (
	"context"
	"errors"
	"testing"

	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/api"
)

func TestGrpcInflightMethodLimiter(t *testing.T) {
	t.Run("nil ingester and distributor receiver", func(t *testing.T) {
		l := newGrpcInflightMethodLimiter(func() ingesterPushReceiver { return nil }, func() distributorPushReceiver { return nil })

		ctx, err := l.RPCCallStarting(context.Background(), "test", nil)
		require.NoError(t, err)
		require.NotPanics(t, func() {
			l.RPCCallFinished(ctx)
		})

		ctx, err = l.RPCCallStarting(context.Background(), ingesterPushMethod, nil)
		require.ErrorIs(t, err, errNoIngester)

		ctx, err = l.RPCCallStarting(context.Background(), httpgrpcHandleMethod, metadata.Pairs(httpgrpc.MetadataMethod, "POST", httpgrpc.MetadataURL, api.PrometheusPushEndpoint))
		require.ErrorIs(t, err, errNoDistributor)

		require.Panics(t, func() {
			// In practice, this will not be called, since l.RPCCallStarting() for ingester push returns error if there's no ingester.
			l.RPCCallFinished(context.WithValue(ctx, pushTypeCtxKey, ingesterPush))
		})

		require.Panics(t, func() {
			// In practice, this will not be called, since l.RPCCallStarting() distributor push returns error if there's no distributor.
			l.RPCCallFinished(context.WithValue(ctx, pushTypeCtxKey, distributorPush))
		})
	})

	t.Run("ingester push receiver", func(t *testing.T) {
		m := &mockIngesterReceiver{}

		l := newGrpcInflightMethodLimiter(func() ingesterPushReceiver { return m }, nil)

		ctx, err := l.RPCCallStarting(context.Background(), "test", nil)
		require.NoError(t, err)
		require.NotPanics(t, func() {
			l.RPCCallFinished(ctx)
		})
		require.Equal(t, 0, m.startCalls)
		require.Equal(t, 0, m.finishCalls)

		ctx, err = l.RPCCallStarting(context.Background(), ingesterPushMethod, nil)
		require.NoError(t, err)
		require.Equal(t, 1, m.startCalls)
		require.Equal(t, 0, m.finishCalls)

		require.NotPanics(t, func() {
			l.RPCCallFinished(ctx)
		})
		require.Equal(t, 1, m.startCalls)
		require.Equal(t, 1, m.finishCalls)

		m.returnError = errors.New("hello there")
		ctx, err = l.RPCCallStarting(context.Background(), ingesterPushMethod, nil)
		require.Error(t, err)
		_, ok := status.FromError(err)
		require.True(t, ok)
		require.Nil(t, ctx.Value(pushTypeCtxKey)) // Original context expected in case of errors.
	})

	t.Run("distributor push via httpgrpc", func(t *testing.T) {
		m := &mockDistributorReceiver{}

		l := newGrpcInflightMethodLimiter(nil, func() distributorPushReceiver { return m })

		ctx, err := l.RPCCallStarting(context.Background(), "test", nil)
		require.NoError(t, err)
		l.RPCCallFinished(ctx)
		require.Equal(t, 0, m.startCalls)
		require.Equal(t, 0, m.finishCalls)

		ctx, err = l.RPCCallStarting(context.Background(), httpgrpcHandleMethod, metadata.New(map[string]string{
			httpgrpc.MetadataMethod:      "POST",
			httpgrpc.MetadataURL:         api.PrometheusPushEndpoint, // no prefix
			grpcutil.MetadataMessageSize: "123456",
		}))
		require.NoError(t, err)
		require.Equal(t, 1, m.startCalls)
		require.Equal(t, 0, m.finishCalls)
		require.Equal(t, int64(123456), m.lastRequestSize)

		// calling finish with empty context does not do any Finish calls.
		l.RPCCallFinished(context.Background())
		require.Equal(t, 1, m.startCalls)
		require.Equal(t, 0, m.finishCalls)

		l.RPCCallFinished(ctx)
		require.Equal(t, 1, m.startCalls)
		require.Equal(t, 1, m.finishCalls)

		// Now return error from distributor receiver.
		m.returnError = errors.New("hello there")
		ctx, err = l.RPCCallStarting(context.Background(), httpgrpcHandleMethod, metadata.New(map[string]string{
			httpgrpc.MetadataMethod:      "POST",
			httpgrpc.MetadataURL:         api.PrometheusPushEndpoint,
			grpcutil.MetadataMessageSize: "123456",
		}))
		require.Error(t, err)
		_, ok := status.FromError(err)
		require.True(t, ok)
		require.Nil(t, ctx.Value(pushTypeCtxKey)) // Original context expected in case of errors.
	})

	t.Run("distributor push via httpgrpc, GET", func(t *testing.T) {
		m := &mockDistributorReceiver{}

		l := newGrpcInflightMethodLimiter(nil, func() distributorPushReceiver { return m })

		_, err := l.RPCCallStarting(context.Background(), httpgrpcHandleMethod, metadata.New(map[string]string{
			httpgrpc.MetadataMethod:      "GET",
			httpgrpc.MetadataURL:         "prefix" + api.PrometheusPushEndpoint,
			grpcutil.MetadataMessageSize: "123456",
		}))
		require.NoError(t, err)
		require.Equal(t, 0, m.startCalls)
		require.Equal(t, 0, m.finishCalls)
		require.Equal(t, int64(0), m.lastRequestSize)
	})

	t.Run("distributor push via httpgrpc, /hello", func(t *testing.T) {
		m := &mockDistributorReceiver{}
		l := newGrpcInflightMethodLimiter(nil, func() distributorPushReceiver { return m })

		_, err := l.RPCCallStarting(context.Background(), httpgrpcHandleMethod, metadata.New(map[string]string{
			httpgrpc.MetadataMethod:      "POST",
			httpgrpc.MetadataURL:         "/hello",
			grpcutil.MetadataMessageSize: "123456",
		}))
		require.NoError(t, err)
		require.Equal(t, 0, m.startCalls)
		require.Equal(t, 0, m.finishCalls)
		require.Equal(t, int64(0), m.lastRequestSize)
	})

	t.Run("distributor push via httpgrpc, wrong message size", func(t *testing.T) {
		m := &mockDistributorReceiver{}
		l := newGrpcInflightMethodLimiter(nil, func() distributorPushReceiver { return m })

		_, err := l.RPCCallStarting(context.Background(), httpgrpcHandleMethod, metadata.New(map[string]string{
			httpgrpc.MetadataMethod:      "POST",
			httpgrpc.MetadataURL:         "prefix" + api.OTLPPushEndpoint,
			grpcutil.MetadataMessageSize: "one-two-three",
		}))
		require.NoError(t, err)
		require.Equal(t, 1, m.startCalls)
		require.Equal(t, 0, m.finishCalls)
		require.Equal(t, int64(0), m.lastRequestSize)
	})
}

type mockIngesterReceiver struct {
	startCalls  int
	finishCalls int
	returnError error
}

func (i *mockIngesterReceiver) StartPushRequest() error {
	i.startCalls++
	return i.returnError
}

func (i *mockIngesterReceiver) FinishPushRequest() {
	i.finishCalls++
}

type mockDistributorReceiver struct {
	startCalls      int
	finishCalls     int
	lastRequestSize int64
	returnError     error
}

func (i *mockDistributorReceiver) StartPushRequest(ctx context.Context, requestSize int64) (context.Context, error) {
	i.startCalls++
	i.lastRequestSize = requestSize
	return ctx, i.returnError
}

func (i *mockDistributorReceiver) FinishPushRequest(_ context.Context) {
	i.finishCalls++
}
