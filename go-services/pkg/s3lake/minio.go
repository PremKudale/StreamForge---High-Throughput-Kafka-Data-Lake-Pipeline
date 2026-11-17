package s3lake

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
)

// MinioClient wraps the S3-compatible MinIO client for data lake storage
type MinioClient struct {
	client     *minio.Client
	bucketName string
	logger     *zap.Logger
}

// NewMinioClient initializes the connection to MinIO / S3
func NewMinioClient(ctx context.Context, logger *zap.Logger) (*MinioClient, error) {
	endpoint := os.Getenv("MINIO_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:9000"
	}
	accessKey := os.Getenv("MINIO_ACCESS_KEY")
	if accessKey == "" {
		accessKey = "minioadmin"
	}
	secretKey := os.Getenv("MINIO_SECRET_KEY")
	if secretKey == "" {
		secretKey = "minioadmin"
	}
	useSSL := os.Getenv("MINIO_USE_SSL") == "true"
	bucketName := os.Getenv("MINIO_DATA_LAKE_BUCKET")
	if bucketName == "" {
		bucketName = "streamforge-data-lake"
	}

	mc, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize MinIO client: %w", err)
	}

	m := &MinioClient{
		client:     mc,
		bucketName: bucketName,
		logger:     logger,
	}

	// Ensure bucket exists
	if err := m.EnsureBucket(ctx); err != nil {
		logger.Warn("Could not ensure bucket exists (MinIO may still be starting up)", zap.Error(err))
	}

	return m, nil
}

// EnsureBucket creates the target bucket if it does not already exist
func (m *MinioClient) EnsureBucket(ctx context.Context) error {
	exists, err := m.client.BucketExists(ctx, m.bucketName)
	if err != nil {
		return err
	}
	if !exists {
		err = m.client.MakeBucket(ctx, m.bucketName, minio.MakeBucketOptions{})
		if err != nil {
			return err
		}
		m.logger.Info("Successfully created MinIO S3 bucket", zap.String("bucket", m.bucketName))
	}
	return nil
}

// GeneratePartitionPath builds a Hive-style partition path:
// e.g. parquet/year=2026/month=11/day=18/hour=03/events_1731901234_uuid.parquet
func GeneratePartitionPath(format string, t time.Time) string {
	utc := t.UTC()
	fileID := uuid.New().String()
	return fmt.Sprintf("%s/year=%04d/month=%02d/day=%02d/hour=%02d/part_%d_%s.%s",
		format,
		utc.Year(),
		utc.Month(),
		utc.Day(),
		utc.Hour(),
		utc.Unix(),
		fileID[:8],
		format,
	)
}

// UploadBytes writes an in-memory byte buffer directly to S3/MinIO
func (m *MinioClient) UploadBytes(ctx context.Context, objectKey string, data []byte, contentType string) (string, error) {
	reader := bytes.NewReader(data)
	size := int64(len(data))

	opts := minio.PutObjectOptions{
		ContentType: contentType,
		UserMetadata: map[string]string{
			"x-amz-meta-source":     "streamforge-lake-sink",
			"x-amz-meta-created-at": time.Now().UTC().Format(time.RFC3339),
		},
	}

	uploadInfo, err := m.client.PutObject(ctx, m.bucketName, objectKey, reader, size, opts)
	if err != nil {
		return "", fmt.Errorf("failed to upload object to s3://%s/%s: %w", m.bucketName, objectKey, err)
	}

	m.logger.Info("Successfully uploaded batch to S3 Data Lake",
		zap.String("bucket", m.bucketName),
		zap.String("key", objectKey),
		zap.Int64("bytes", uploadInfo.Size),
		zap.String("etag", uploadInfo.ETag),
	)

	return fmt.Sprintf("s3://%s/%s", m.bucketName, objectKey), nil
}

