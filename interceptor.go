package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"go.opentelemetry.io/otel/trace"
	"gofr.dev/pkg/gofr/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	statusCodeWidth   = 3
	responseTimeWidth = 11
)

func rpcLogger(logger logging.Logger) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()
		err := invoker(ctx, method, req, reply, cc, opts...)
		duration := time.Since(start)

		logEntry := &gRPCLog{
			ID:           trace.SpanFromContext(ctx).SpanContext().TraceID().String(),
			StartTime:    start.Format("2006-01-02T15:04:05.999999999-07:00"),
			ResponseTime: duration.Microseconds(),
			Method:       method,
		}

		if err != nil {
			statusErr, _ := status.FromError(err)
			logEntry.StatusCode = int32(statusErr.Code())
		} else {
			logEntry.StatusCode = int32(codes.OK)
		}

		logger.Info(logEntry)

		return err
	}
}

type gRPCLog struct {
	ID           string `json:"id"`
	StartTime    string `json:"startTime"`
	ResponseTime int64  `json:"responseTime"`
	Method       string `json:"method"`
	StatusCode   int32  `json:"statusCode"`
	StreamType   string `json:"streamType,omitempty"`
}

func (l *gRPCLog) PrettyPrint(writer io.Writer) {
	streamInfo := ""
	if l.StreamType != "" {
		streamInfo = fmt.Sprintf(" [%s]", l.StreamType)
	}

	fmt.Fprintf(writer, "\u001B[38;5;8m%s \u001B[38;5;%dm%-*d"+
		"\u001B[0m %*d\u001B[38;5;8mµs\u001B[0m %s%s %s\n",
		l.ID, colorForGRPCCode(l.StatusCode),
		statusCodeWidth, l.StatusCode,
		responseTimeWidth, l.ResponseTime,
		"GRPC", streamInfo, l.Method)
}

func (l gRPCLog) String() string {
	line, _ := json.Marshal(l)
	return string(line)
}

func colorForGRPCCode(s int32) int {
	const (
		blue = 34
		red  = 202
	)

	if s == 0 {
		return blue
	}

	return red
}
