package client

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/IBM/sarama"
	"github.com/spf13/viper"
)

// buildBaseConfigFromViper builds a *sarama.Config from viper for a given kafka connection name.
// It only sets connection-level options (version/client_id/sasl/tls) plus a couple of safe defaults.
// Per-listener/per-producer knobs should be applied via ListenerDefaultConfig/PublisherDefaultConfig
// and/or WithConfig().
func buildBaseConfigFromViper(cfg *viper.Viper, connectName string) (*sarama.Config, error) {
	// Start from a clean Sarama config. We'll merge this into defaults later.
	sc := sarama.NewConfig()

	prefix := "kafka." + connectName + "."

	if v := strings.TrimSpace(cfg.GetString(prefix + "client_id")); v != "" {
		sc.ClientID = v
	}

	if v := strings.TrimSpace(cfg.GetString(prefix + "version")); v != "" {
		ver, err := sarama.ParseKafkaVersion(v)
		if err != nil {
			return nil, fmt.Errorf("kafka.%s.version parse: %w", connectName, err)
		}
		sc.Version = ver
	}

	// Consumer offsets (connection-level in your case; can still be overridden by WithConfig).
	// Values: oldest|newest
	if v := strings.TrimSpace(cfg.GetString(prefix + "consumer.offsets.initial")); v != "" {
		switch strings.ToLower(v) {
		case "oldest":
			sc.Consumer.Offsets.Initial = sarama.OffsetOldest
		case "newest":
			sc.Consumer.Offsets.Initial = sarama.OffsetNewest
		default:
			return nil, fmt.Errorf("kafka.%s.consumer.offsets.initial: unknown value %q (use oldest|newest)", connectName, v)
		}
	}

	// SASL
	if cfg.IsSet(prefix+"sasl.enabled") && cfg.GetBool(prefix+"sasl.enabled") {
		sc.Net.SASL.Enable = true
		sc.Net.SASL.User = cfg.GetString(prefix + "sasl.username")
		sc.Net.SASL.Password = cfg.GetString(prefix + "sasl.password")
		if strings.TrimSpace(sc.Net.SASL.User) == "" {
			return nil, fmt.Errorf("kafka.%s.sasl.username is required when sasl.enabled=true", connectName)
		}
		if strings.TrimSpace(sc.Net.SASL.Password) == "" {
			return nil, fmt.Errorf("kafka.%s.sasl.password is required when sasl.enabled=true", connectName)
		}

		// Default Sarama mechanism is PLAIN. Support explicit value too.
		if mech := strings.TrimSpace(cfg.GetString(prefix + "sasl.mechanism")); mech != "" {
			switch strings.ToUpper(mech) {
			case "PLAIN":
				sc.Net.SASL.Mechanism = sarama.SASLTypePlaintext
			case "SCRAM-SHA-256":
				sc.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
				sc.Net.SASL.SCRAMClientGeneratorFunc = scramSHA256ClientGenerator
				sc.Net.SASL.Version = sarama.SASLHandshakeV1
			case "SCRAM-SHA-512":
				sc.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
				sc.Net.SASL.SCRAMClientGeneratorFunc = scramSHA512ClientGenerator
				sc.Net.SASL.Version = sarama.SASLHandshakeV1
			default:
				return nil, fmt.Errorf("kafka.%s.sasl.mechanism: unsupported %q (supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)", connectName, mech)
			}
		}
	}

	// TLS
	if cfg.IsSet(prefix+"tls.enabled") && cfg.GetBool(prefix+"tls.enabled") {
		tlsCfg, err := buildTLSConfig(cfg, prefix+"tls.")
		if err != nil {
			return nil, fmt.Errorf("kafka.%s.tls: %w", connectName, err)
		}
		sc.Net.TLS.Enable = true
		sc.Net.TLS.Config = tlsCfg
	}

	return sc, nil
}

func buildTLSConfig(cfg *viper.Viper, prefix string) (*tls.Config, error) {
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	if cfg.IsSet(prefix + "insecure_skip_verify") {
		tlsCfg.InsecureSkipVerify = cfg.GetBool(prefix + "insecure_skip_verify")
	}

	// CA: either path or PEM string
	if caPath := strings.TrimSpace(cfg.GetString(prefix + "ca_file")); caPath != "" {
		b, err := os.ReadFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("read ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(b) {
			return nil, fmt.Errorf("append ca_file pem: failed")
		}
		tlsCfg.RootCAs = pool
	} else if caPEM := strings.TrimSpace(cfg.GetString(prefix + "ca_pem")); caPEM != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(caPEM)) {
			return nil, fmt.Errorf("append ca_pem: failed")
		}
		tlsCfg.RootCAs = pool
	}

	certFile := strings.TrimSpace(cfg.GetString(prefix + "cert_file"))
	keyFile := strings.TrimSpace(cfg.GetString(prefix + "key_file"))
	certPEM := strings.TrimSpace(cfg.GetString(prefix + "cert_pem"))
	keyPEM := strings.TrimSpace(cfg.GetString(prefix + "key_pem"))

	// Client cert optional
	if certFile != "" || keyFile != "" || certPEM != "" || keyPEM != "" {
		switch {
		case certFile != "" && keyFile != "":
			crt, err := tls.LoadX509KeyPair(certFile, keyFile)
			if err != nil {
				return nil, fmt.Errorf("load cert/key file: %w", err)
			}
			tlsCfg.Certificates = []tls.Certificate{crt}
		case certPEM != "" && keyPEM != "":
			crt, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
			if err != nil {
				return nil, fmt.Errorf("parse cert/key pem: %w", err)
			}
			tlsCfg.Certificates = []tls.Certificate{crt}
		default:
			return nil, fmt.Errorf("cert/key must be both set (files or pem)")
		}
	}

	if sni := strings.TrimSpace(cfg.GetString(prefix + "server_name")); sni != "" {
		tlsCfg.ServerName = sni
	}

	return tlsCfg, nil
}
