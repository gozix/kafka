package client

import (
	"crypto/sha256"
	"crypto/sha512"
	"hash"

	"github.com/IBM/sarama"
	"github.com/xdg/scram"
)

// SCRAM client implementation for Sarama using github.com/xdg/scram.
// Based on Sarama examples.

type scramClient struct {
	conv    *scram.ClientConversation
	hashGen scram.HashGeneratorFcn
}

func (x *scramClient) Begin(userName, password, authzID string) error {
	c, err := x.hashGen.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.conv = c.NewConversation()
	return nil
}

func (x *scramClient) Step(challenge string) (string, error) {
	return x.conv.Step(challenge)
}

func (x *scramClient) Done() bool {
	return x.conv.Done()
}

func scramSHA256ClientGenerator() sarama.SCRAMClient {
	return &scramClient{hashGen: scramSHA256Hash}
}

func scramSHA512ClientGenerator() sarama.SCRAMClient {
	return &scramClient{hashGen: scramSHA512Hash}
}

var scramSHA256Hash scram.HashGeneratorFcn = func() hash.Hash { return sha256.New() }
var scramSHA512Hash scram.HashGeneratorFcn = func() hash.Hash { return sha512.New() }
