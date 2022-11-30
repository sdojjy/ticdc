// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package sarama

import (
	"context"
	"crypto/tls"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/sink/mq/producer/kafka"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/sink/kafka/metadata"
	"go.uber.org/zap"
)

type saramaClusterAdmin struct {
	admin sarama.ClusterAdmin
}

// NewClusterAdmin creates a new ClusterAdmin using the given broker addresses and configuration.
func NewClusterAdmin(addrs []string, conf *sarama.Config) (*saramaClusterAdmin, error) {
	client, err := sarama.NewClient(addrs, conf)
	if err != nil {
		return nil, err
	}
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		client.Close()
	}
	return &saramaClusterAdmin{admin: admin}, err
}

// ListTopics list the topics available in the cluster with the default options.
func (s *saramaClusterAdmin) ListTopics() (map[string]metadata.TopicDetail, error) {
	interval, err := s.admin.ListTopics()
	if err != nil {
		return nil, err
	}
	m := make(map[string]metadata.TopicDetail, len(interval))
	for topic, detail := range interval {
		m[topic] = metadata.TopicDetail{
			NumPartitions:     detail.NumPartitions,
			ReplicationFactor: detail.ReplicationFactor,
			ReplicaAssignment: detail.ReplicaAssignment,
			ConfigEntries:     detail.ConfigEntries,
		}
	}
	return m, nil
}

// DescribeCluster gets information about the nodes in the cluster
func (s *saramaClusterAdmin) DescribeCluster() (brokers []*sarama.Broker, controllerID int32, err error) {

}

// DescribeConfig gets the configuration for the specified resources.
func (s *saramaClusterAdmin) DescribeConfig(resource sarama.ConfigResource) ([]sarama.ConfigEntry, error) {

}

// DescribeTopics fetches metadata from some topics.
func (s *saramaClusterAdmin) DescribeTopics(topics []string) (metadata []*sarama.TopicMetadata, err error) {

}

// CreateTopic creates a new topic.
func (s *saramaClusterAdmin) CreateTopic(topic string, detail *metadata.TopicDetail, validateOnly bool) error {
	intervalDetail := sarama.TopicDetail{
		NumPartitions:     detail.NumPartitions,
		ReplicationFactor: detail.ReplicationFactor,
		ReplicaAssignment: detail.ReplicaAssignment,
		ConfigEntries:     detail.ConfigEntries,
	}
	return s.admin.CreateTopic(topic, &intervalDetail, validateOnly)
}

// Close shuts down the admin and closes underlying client.
func (s *saramaClusterAdmin) Close() error {
	return s.admin.Close()
}

// NewSaramaConfig return the default config and set the according version and metrics
func NewSaramaConfig(ctx context.Context, c *metadata.Config) (*sarama.Config, error) {
	config := sarama.NewConfig()

	version, err := sarama.ParseKafkaVersion(c.Version)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidVersion, err)
	}
	var role string
	if contextutil.IsOwnerFromCtx(ctx) {
		role = "owner"
	} else {
		role = "processor"
	}
	captureAddr := contextutil.CaptureAddrFromCtx(ctx)
	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)

	config.ClientID, err = kafka.kafkaClientID(role, captureAddr, changefeedID, c.ClientID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	config.Version = version

	// Producer fetch metadata from brokers frequently, if metadata cannot be
	// refreshed easily, this would indicate the network condition between the
	// capture server and kafka broker is not good.
	// In the scenario that cannot get response from Kafka server, this default
	// setting can help to get response more quickly.
	config.Metadata.Retry.Max = 1
	config.Metadata.Retry.Backoff = 100 * time.Millisecond
	// This Timeout is useless if the `RefreshMetadata` time cost is less than it.
	config.Metadata.Timeout = 1 * time.Minute

	// Admin.Retry take effect on `ClusterAdmin` related operations,
	// only `CreateTopic` for cdc now. set the `Timeout` to `1m` to make CI stable.
	config.Admin.Retry.Max = 5
	config.Admin.Retry.Backoff = 100 * time.Millisecond
	config.Admin.Timeout = 1 * time.Minute

	// Producer.Retry take effect when the producer try to send message to kafka
	// brokers. If kafka cluster is healthy, just the default value should be enough.
	// For kafka cluster with a bad network condition, producer should not try to
	// waster too much time on sending a message, get response no matter success
	// or fail as soon as possible is preferred.
	config.Producer.Retry.Max = 3
	config.Producer.Retry.Backoff = 100 * time.Millisecond

	// make sure sarama producer flush messages as soon as possible.
	config.Producer.Flush.Bytes = 0
	config.Producer.Flush.Messages = 0
	config.Producer.Flush.Frequency = time.Duration(0)

	config.Net.DialTimeout = c.DialTimeout
	config.Net.WriteTimeout = c.WriteTimeout
	config.Net.ReadTimeout = c.ReadTimeout

	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.MaxMessageBytes = c.MaxMessageBytes
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	compression := strings.ToLower(strings.TrimSpace(c.Compression))
	switch compression {
	case "none":
		config.Producer.Compression = sarama.CompressionNone
	case "gzip":
		config.Producer.Compression = sarama.CompressionGZIP
	case "snappy":
		config.Producer.Compression = sarama.CompressionSnappy
	case "lz4":
		config.Producer.Compression = sarama.CompressionLZ4
	case "zstd":
		config.Producer.Compression = sarama.CompressionZSTD
	default:
		log.Warn("Unsupported compression algorithm", zap.String("compression", c.Compression))
		config.Producer.Compression = sarama.CompressionNone
	}
	if config.Producer.Compression != sarama.CompressionNone {
		log.Info("Kafka producer uses " + compression + " compression algorithm")
	}

	if c.EnableTLS {
		// for SSL encryption with a trust CA certificate, we must populate the
		// following two params of config.Net.TLS
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{
			MinVersion: tls.VersionTLS12,
			NextProtos: []string{"h2", "http/1.1"},
		}

		// for SSL encryption with self-signed CA certificate, we reassign the
		// config.Net.TLS.Config using the relevant credential files.
		if c.Credential != nil && c.Credential.IsTLSEnabled() {
			config.Net.TLS.Config, err = c.Credential.ToTLSConfig()
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}

	completeSaramaSASLConfig(config, c)

	return config, err
}

func completeSaramaSASLConfig(config *sarama.Config, c *metadata.Config) {
	if c.SASL != nil && c.SASL.SASLMechanism != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.Mechanism = sarama.SASLMechanism(c.SASL.SASLMechanism)
		switch c.SASL.SASLMechanism {
		case sarama.SASLTypeSCRAMSHA256, sarama.SASLTypeSCRAMSHA512, sarama.SASLTypePlaintext:
			config.Net.SASL.User = c.SASL.SASLUser
			config.Net.SASL.Password = c.SASL.SASLPassword
			if strings.EqualFold(string(c.SASL.SASLMechanism), sarama.SASLTypeSCRAMSHA256) {
				config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
					return &security.XDGSCRAMClient{HashGeneratorFcn: security.SHA256}
				}
			} else if strings.EqualFold(string(c.SASL.SASLMechanism), sarama.SASLTypeSCRAMSHA512) {
				config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
					return &security.XDGSCRAMClient{HashGeneratorFcn: security.SHA512}
				}
			}
		case sarama.SASLTypeGSSAPI:
			config.Net.SASL.GSSAPI.AuthType = int(c.SASL.GSSAPI.AuthType)
			config.Net.SASL.GSSAPI.Username = c.SASL.GSSAPI.Username
			config.Net.SASL.GSSAPI.ServiceName = c.SASL.GSSAPI.ServiceName
			config.Net.SASL.GSSAPI.KerberosConfigPath = c.SASL.GSSAPI.KerberosConfigPath
			config.Net.SASL.GSSAPI.Realm = c.SASL.GSSAPI.Realm
			config.Net.SASL.GSSAPI.DisablePAFXFAST = c.SASL.GSSAPI.DisablePAFXFAST
			switch c.SASL.GSSAPI.AuthType {
			case security.UserAuth:
				config.Net.SASL.GSSAPI.Password = c.SASL.GSSAPI.Password
			case security.KeyTabAuth:
				config.Net.SASL.GSSAPI.KeyTabPath = c.SASL.GSSAPI.KeyTabPath
			}
		}
	}
}
