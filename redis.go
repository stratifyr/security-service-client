package client

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
	"gofr.dev/pkg/gofr/config"
)

func newSecurityServiceRedisClient(config config.Config) (*redis.Client, error) {
	host := config.Get("SECURITY_SERVICE_CLIENT_REDIS_HOST")
	if host == "" {
		return nil, errors.New("SECURITY_SERVICE_CLIENT_REDIS_HOST is required")
	}

	port := config.Get("SECURITY_SERVICE_CLIENT_REDIS_PORT")
	if port == "" {
		return nil, errors.New("SECURITY_SERVICE_CLIENT_REDIS_PORT is required")
	}

	dbStr := config.Get("SECURITY_SERVICE_CLIENT_REDIS_DB")
	if dbStr == "" {
		return nil, errors.New("SECURITY_SERVICE_CLIENT_REDIS_DB is required")
	}

	db, _ := strconv.Atoi(dbStr)
	rc := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%s", host, port),
		DB:   db,
	})

	if err := rc.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("could not connect to redis at '%s:%s', err: %s", host, port, err)
	}

	return rc, nil
}
