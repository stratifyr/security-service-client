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
	"gofr.dev/pkg/gofr/metrics"
	googleproto "google.golang.org/protobuf/proto"

	gofrWrapper "github.com/stratifyr/security-service-proto/go/gofr-wrapper"
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
	UpdateSecurityLTP(ctx *gofr.Context, id int32, ltp float64) error
	CreateOrUpdateSecurityStat(ctx *gofr.Context, payload *pb.CreateOrUpdateSecurityStatRequest) error
	GetMarketDataJobs(ctx *gofr.Context, status string) ([]*pb.MarketDataJob, error)
	UpdateMarketDataJobStatus(ctx *gofr.Context, id int32, status string, logs any) error
}

type securityServiceClient struct {
	grpcConn gofrWrapper.SecurityServiceGoFrClient
	cache    *redis.Client
}

func NewSecurityServiceClient(config config.Config, metricsManager metrics.Manager) (SecurityServiceClient, error) {
	securityServiceHost := config.Get("SECURITY_SERVICE_GRPC_HOST")
	if securityServiceHost == "" {
		return nil, errors.New("SECURITY_SERVICE_GRPC_HOST is required")
	}

	grpcConn, err := gofrWrapper.NewSecurityServiceGoFrClient(securityServiceHost, metricsManager)
	if err != nil {
		return nil, fmt.Errorf("failed to create grpc connection, %s", err.Error())
	}

	redisClient, err := newSecurityServiceRedisClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create redis client, %s", err.Error())
	}

	return &securityServiceClient{
		grpcConn: grpcConn,
		cache:    redisClient,
	}, nil
}

func (c *securityServiceClient) GetMarketDays(ctx *gofr.Context, startDate, endDate time.Time) ([]time.Time, error) {
	resp, err := c.grpcConn.GetMarketDays(ctx, &pb.GetMarketDaysRequest{
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

	resp, err := c.grpcConn.GetMetrics(ctx, &pb.GetMetricsRequest{})
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

	resp, err := c.grpcConn.GetSecurities(ctx, &pb.GetSecuritiesRequest{Date: date.Format(time.DateOnly)})
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

func (c *securityServiceClient) UpdateSecurityLTP(ctx *gofr.Context, id int32, ltp float64) error {
	_, err := c.grpcConn.UpdateSecurity(ctx, &pb.UpdateSecurityRequest{Id: id, Ltp: ltp})
	if err != nil {
		return fmt.Errorf("failed rpc /security-service/UpdateSecurity, %s", err.Error())
	}

	return nil
}

func (c *securityServiceClient) CreateOrUpdateSecurityStat(ctx *gofr.Context, payload *pb.CreateOrUpdateSecurityStatRequest) error {
	_, err := c.grpcConn.CreateOrUpdateSecurityStat(ctx, payload)
	if err != nil {
		return fmt.Errorf("failed rpc /security-service/CreateOrUpdateSecurityStat, %s", err.Error())
	}

	return nil
}

func (c *securityServiceClient) GetMarketDataJobs(ctx *gofr.Context, status string) ([]*pb.MarketDataJob, error) {
	resp, err := c.grpcConn.GetMarketDataJobs(ctx, &pb.GetMarketDataJobsRequest{Status: status})
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

	_, err = c.grpcConn.UpdateMarketDataJob(ctx, &pb.UpdateMarketDataJobRequest{Id: id, Status: status, Logs: logBytes})
	if err != nil {
		return fmt.Errorf("failed rpc /security-service/UpdateMarketDataJob, %s", err.Error())
	}

	return nil
}
