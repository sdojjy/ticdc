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

package debezium

import (
	"context"
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"go.uber.org/zap"
)

// NewDebeziumRowEventEncoderBuilder creates a debezium encoderBuilder.
func NewDebeziumRowEventEncoderBuilder(
	ctx context.Context, config *common.Config,
) (codec.RowEventEncoderBuilder, error) {
	return &encoderBuilder{}, nil
}

type encoderBuilder struct {
}

func (e *encoderBuilder) Build() codec.RowEventEncoder {
	return &debeziumRowEventEncoder{}
}

func (e *encoderBuilder) CleanMetrics() {
}

type debeziumRowEventEncoder struct {
	messages []*common.Message
	config   *common.Config
}

func (d *debeziumRowEventEncoder) EncodeCheckpointEvent(ts uint64) (*common.Message, error) {
	return nil, nil
}

func (d *debeziumRowEventEncoder) EncodeDDLEvent(e *model.DDLEvent) (*common.Message, error) {
	message := d.newJSONMessageForDDL(e)
	value, err := json.Marshal(message)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
	}
	value, err = common.Compress(
		d.config.ChangefeedID, d.config.LargeMessageHandle.LargeMessageHandleCompression, value,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return common.NewDDLMsg(config.ProtocolDebezium, nil, value, e), nil
}

func (d *debeziumRowEventEncoder) AppendRowChangedEvent(ctx context.Context,
	s string,
	e *model.RowChangedEvent,
	callback func()) error {
	message := d.newJSONMessageForDML(e)
	value, err := json.Marshal(message)
	if err != nil {
		return errors.Trace(err)
	}

	m := &common.Message{
		Key:      nil,
		Value:    value,
		Ts:       e.CommitTs,
		Schema:   &e.Table.Schema,
		Table:    &e.Table.Table,
		Type:     model.MessageTypeRow,
		Protocol: config.ProtocolDebezium,
		Callback: callback,
	}
	m.IncRowsCount()

	originLength := m.Length()
	if m.Length() > d.config.MaxMessageBytes {
		log.Error("Single Message is too large for debezium",
			zap.Int("maxMessageBytes", d.config.MaxMessageBytes),
			zap.Int("length", originLength))
		return cerror.ErrMessageTooLarge.GenWithStackByArgs()
	}

	d.messages = append(d.messages, m)
	return nil
}

func (d *debeziumRowEventEncoder) Build() []*common.Message {
	if len(d.messages) == 0 {
		return nil
	}

	result := d.messages
	d.messages = nil
	return result
}

func (d *debeziumRowEventEncoder) newJSONMessageForDDL(e *model.DDLEvent) *ddlPayload {
	return &ddlPayload{}
}

func (d *debeziumRowEventEncoder) newJSONMessageForDML(e *model.RowChangedEvent) *DMLPayload {
	return NewDMLPayloadBuilder().WithRowChangedEvent(e).Build()
}
