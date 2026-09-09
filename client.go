package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"
	"gofr.dev/pkg/gofr"
	"gofr.dev/pkg/gofr/config"
	"gofr.dev/pkg/gofr/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	googleproto "google.golang.org/protobuf/proto"

	"github.com/stratifyr/security-service-proto/go/pb"
)

const (
	SecuritiesCacheKey = "security-service:client-cache:securities:date:%s"
	MetricsCacheKey    = "security-service:client-cache:metrics"
)

type SecurityServiceClient interface {
	GetMarketDays(ctx *gofr.Context, startDate, endDate time.Time) ([]time.Time, error)
	GetMetrics(ctx *gofr.Context) ([]*pb.Metric, error)
	GetSecurities(ctx *gofr.Context, date time.Time) ([]*pb.Security, error)
	UpdateSecurity(ctx *gofr.Context, payload *pb.UpdateSecurityRequest) error
	CreateOrUpdateSecurityStat(ctx *gofr.Context, payload *pb.CreateOrUpdateSecurityStatRequest) error
	GetIndices(ctx *gofr.Context, date time.Time) ([]*pb.Index, error)
	UpsertIndex(ctx *gofr.Context, payload *pb.UpsertIndexRequest) error
	UpsertIndexStat(ctx *gofr.Context, payload *pb.UpsertIndexStatRequest) error
	GetMarketDataJobs(ctx *gofr.Context, status string) ([]*pb.MarketDataJob, error)
	UpdateMarketDataJobStatus(ctx *gofr.Context, id int32, status string, logs any) error

	Close() error
}

type securityServiceClient struct {
	grpcConn *grpc.ClientConn
	client   pb.SecurityServiceClient
	cache    *redis.Client
}

func NewSecurityServiceClient(config config.Config, logger logging.Logger) (SecurityServiceClient, error) {
	securityServiceHost := config.Get("SECURITY_SERVICE_GRPC_HOST")
	if securityServiceHost == "" {
		return nil, errors.New("SECURITY_SERVICE_GRPC_HOST is required")
	}

	grpcConn, err := grpc.NewClient(securityServiceHost,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(rpcLogger(logger)))
	if err != nil {
		return nil, fmt.Errorf("failed to create grpc connection, %s", err.Error())
	}

	redisClient, err := newSecurityServiceRedisClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create redis client, %s", err.Error())
	}

	return &securityServiceClient{
		grpcConn: grpcConn,
		client:   pb.NewSecurityServiceClient(grpcConn),
		cache:    redisClient,
	}, nil
}

func (c *securityServiceClient) GetMarketDays(ctx *gofr.Context, startDate, endDate time.Time) ([]time.Time, error) {
	resp, err := c.client.GetMarketDays(ctx, &pb.GetMarketDaysRequest{
		StartDate: startDate.Format(time.DateOnly),
		EndDate:   endDate.Format(time.DateOnly),
	})
	if err != nil {
		return nil, fmt.Errorf("failed rpc /security-service/GetMarketDays, %s", err.Error())
	}

	var marketDays = make([]time.Time, len(resp.Days))

	for i := range resp.Days {
		marketDays[i], _ = time.Parse(time.DateOnly, resp.Days[i])
	}

	sort.Slice(marketDays, func(i, j int) bool { return marketDays[i].Before(marketDays[j]) })

	return marketDays, nil
}

func (c *securityServiceClient) GetMetrics(ctx *gofr.Context) ([]*pb.Metric, error) {
	bytes, err := c.cache.Get(ctx, MetricsCacheKey).Bytes()
	if err == nil {
		var result pb.GetMetricsResponse

		if err = googleproto.Unmarshal(bytes, &result); err == nil {
			return result.GetMetrics(), nil
		}
	}

	ctx.Logger.Warnf("client cache miss, key: %s, err: %s", MetricsCacheKey, err.Error())

	resp, err := c.client.GetMetrics(ctx, &pb.GetMetricsRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed rpc /security-service/GetMetrics, %s", err.Error())
	}

	bytes, err = googleproto.Marshal(resp)
	if err == nil {
		if err = c.cache.Set(ctx, MetricsCacheKey, bytes, 30*24*time.Hour).Err(); err != nil {
			ctx.Logger.Warnf("failed to client cache, key: %s, err: %s", MetricsCacheKey, err.Error())
		}
	}

	return resp.Metrics, nil
}

func (c *securityServiceClient) GetSecurities(ctx *gofr.Context, date time.Time) ([]*pb.Security, error) {
	key := fmt.Sprintf(SecuritiesCacheKey, date.Format(time.DateOnly))

	bytes, err := c.cache.Get(ctx, key).Bytes()
	if err == nil {
		var result pb.GetSecuritiesResponse

		if err = googleproto.Unmarshal(bytes, &result); err == nil {
			return result.GetSecurities(), nil
		}
	}

	ctx.Logger.Warnf("client cache miss, key: %s, err: %s", key, err.Error())

	resp, err := c.client.GetSecurities(ctx, &pb.GetSecuritiesRequest{Date: date.Format(time.DateOnly)})
	if err != nil {
		return nil, fmt.Errorf("failed rpc /security-service/GetSecurities, %s", err.Error())
	}

	bytes, err = googleproto.Marshal(resp)
	if err == nil {
		if err = c.cache.Set(ctx, key, bytes, 30*24*time.Hour).Err(); err != nil {
			ctx.Logger.Warnf("failed to client cache, key: %s, err: %s", key, err.Error())
		}
	}

	return resp.Securities, nil
}

func (c *securityServiceClient) UpdateSecurity(ctx *gofr.Context, payload *pb.UpdateSecurityRequest) error {
	_, err := c.client.UpdateSecurity(ctx, payload)
	if err != nil {
		return fmt.Errorf("failed rpc /security-service/UpdateSecurity, %s", err.Error())
	}

	return nil
}

func (c *securityServiceClient) CreateOrUpdateSecurityStat(ctx *gofr.Context, payload *pb.CreateOrUpdateSecurityStatRequest) error {
	if _, err := c.client.CreateOrUpdateSecurityStat(ctx, payload); err != nil {
		return fmt.Errorf("failed rpc /security-service/CreateOrUpdateSecurityStat, %s", err.Error())
	}

	return nil
}

func (c *securityServiceClient) GetIndices(ctx *gofr.Context, date time.Time) ([]*pb.Index, error) {
	resp, err := c.client.GetIndices(ctx, &pb.GetIndicesRequest{Date: date.Format(time.DateOnly)})
	if err != nil {
		return nil, fmt.Errorf("failed rpc /security-service/GetIndices, %s", err.Error())
	}

	return resp.Indices, nil
}

func (c *securityServiceClient) UpsertIndex(ctx *gofr.Context, payload *pb.UpsertIndexRequest) error {
	_, err := c.client.UpsertIndex(ctx, payload)
	if err != nil {
		return fmt.Errorf("failed rpc /security-service/UpsertIndex, %s", err.Error())
	}

	return nil
}

func (c *securityServiceClient) UpsertIndexStat(ctx *gofr.Context, payload *pb.UpsertIndexStatRequest) error {
	_, err := c.client.UpsertIndexStat(ctx, payload)
	if err != nil {
		return fmt.Errorf("failed rpc /security-service/UpsertIndexStat, %s", err.Error())
	}

	return nil
}

func (c *securityServiceClient) GetMarketDataJobs(ctx *gofr.Context, status string) ([]*pb.MarketDataJob, error) {
	resp, err := c.client.GetMarketDataJobs(ctx, &pb.GetMarketDataJobsRequest{Status: status})
	if err != nil {
		return nil, fmt.Errorf("failed rpc /security-service/GetMarketDataJobs, %s", err.Error())
	}

	return resp.MarketDataJobs, nil
}

func (c *securityServiceClient) UpdateMarketDataJobStatus(ctx *gofr.Context, id int32, status string, logs any) error {
	logBytes, err := json.Marshal(logs)
	if err != nil {
		return fmt.Errorf("invalid log format, %s", err.Error())
	}

	_, err = c.client.UpdateMarketDataJob(ctx, &pb.UpdateMarketDataJobRequest{Id: id, Status: status, Logs: logBytes})
	if err != nil {
		return fmt.Errorf("failed rpc /security-service/UpdateMarketDataJob, %s", err.Error())
	}

	return nil
}

func (c *securityServiceClient) Close() error {
	return c.grpcConn.Close()
}
