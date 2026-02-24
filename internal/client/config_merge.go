package client

import "github.com/IBM/sarama"

// mergeSaramaConfig merges values from src into dst.
// It is intentionally conservative and only covers fields we set via config/viper in this package.
// If you need more fields, extend it explicitly.
func mergeSaramaConfig(dst, src *sarama.Config) {
	if dst == nil || src == nil {
		return
	}

	// Basic
	if src.ClientID != "" {
		dst.ClientID = src.ClientID
	}
	if src.Version != (sarama.KafkaVersion{}) {
		dst.Version = src.Version
	}

	// Consumer.Offsets.Initial: sarama default is OffsetNewest (-1), OffsetOldest is -2.
	// We only override if explicitly set to OffsetOldest, since OffsetNewest is already
	// the default and we cannot distinguish "not set" from "set to newest".
	if src.Consumer.Offsets.Initial == sarama.OffsetOldest {
		dst.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	// Network TLS/SASL
	if src.Net.TLS.Enable {
		dst.Net.TLS.Enable = true
		dst.Net.TLS.Config = src.Net.TLS.Config
	}
	if src.Net.SASL.Enable {
		dst.Net.SASL.Enable = true
		dst.Net.SASL.User = src.Net.SASL.User
		dst.Net.SASL.Password = src.Net.SASL.Password
		if src.Net.SASL.Mechanism != "" {
			dst.Net.SASL.Mechanism = src.Net.SASL.Mechanism
		}
		if src.Net.SASL.Version != 0 {
			dst.Net.SASL.Version = src.Net.SASL.Version
		}
		if src.Net.SASL.SCRAMClientGeneratorFunc != nil {
			dst.Net.SASL.SCRAMClientGeneratorFunc = src.Net.SASL.SCRAMClientGeneratorFunc
		}
	}
}
