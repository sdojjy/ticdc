package model

import (
	"encoding/json"

	"github.com/pingcap/errors"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

type UpstreamID struct {
	Namespace string
	ClusterID string
}

// UpstreamInfo store in etcd.
type UpstreamInfo struct {
	ClusterID string `json:"cluster-id"`
	PD        string `json:"pd"`
	Key       string `json:"version"`
	CA        string `json:"ca"`
	Cert      string `json:"cert"`
}

// Marshal using json.Marshal.
func (c *UpstreamInfo) Marshal() ([]byte, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrMarshalFailed, err)
	}

	return data, nil
}

// Unmarshal from binary data.
func (c *UpstreamInfo) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, c)
	return errors.Annotatef(cerror.WrapError(cerror.ErrUnmarshalFailed, err),
		"unmarshal data: %v", data)
}
