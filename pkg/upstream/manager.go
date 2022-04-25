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

package upstream

import (
	"context"
	"strings"
	"sync"

	"github.com/benbjohnson/clock"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"go.uber.org/zap"
)

var UpStreamManager *Manager

type Manager struct {
	ups sync.Map
	c   clock.Clock
	ctx context.Context
	lck sync.Mutex
}

// NewManager create a new Manager.
func NewManager(ctx context.Context) *Manager {
	return &Manager{c: clock.New(), ctx: ctx}
}

func (m *Manager) TryInit(clusterID string, upstreamInfo *model.UpstreamInfo) error {
	if up, ok := m.ups.Load(clusterID); ok {
		up.(*UpStream).hold()
		up.(*UpStream).clearIdealCount()
		return nil
	}
	m.lck.Lock()
	defer m.lck.Unlock()
	if up, ok := m.ups.Load(clusterID); ok {
		up.(*UpStream).hold()
		up.(*UpStream).clearIdealCount()
		return nil
	}

	// TODO: use changefeed's pd addr in the future
	pdEndpoints := strings.Split(upstreamInfo.PD, ",")
	securityConfig := &config.SecurityConfig{
		CAPath:   upstreamInfo.CAPath,
		CertPath: upstreamInfo.CertPath,
		KeyPath:  upstreamInfo.KeyPath,
	}
	up := newUpStream(pdEndpoints, securityConfig)
	up.hold()
	// 之后的实现需要检查错误
	_ = up.Init(m.ctx)
	m.ups.Store(clusterID, up)
	return nil
}

// Get gets a upStream by clusterID, if this upStream does not exis, create it.
func (m *Manager) Get(clusterID string) (*UpStream, error) {
	if up, ok := m.ups.Load(clusterID); ok {
		up.(*UpStream).hold()
		up.(*UpStream).clearIdealCount()
		return up.(*UpStream), nil
	}
	return nil, errors.New("upstream is not found")
}

// Release releases a upStream by clusterID
func (m *Manager) Release(clusterID uint64) {
	if up, ok := m.ups.Load(clusterID); ok {
		up.(*UpStream).unhold()
	}
}

func (m *Manager) checkUpstreams() {
	m.ups.Range(func(k, v interface{}) bool {
		up := v.(*UpStream)
		if !up.isHold() {
			up.addIdealCount()
		}
		if up.shouldClose() {
			up.close()
			m.ups.Delete(k)
		}
		return true
	})
}

func (m *Manager) closeUpstreams() {
	m.ups.Range(func(k, v interface{}) bool {
		id := k.(uint64)
		up := v.(*UpStream)
		up.close()
		log.Info("upStream closed", zap.Uint64("cluster id", id))
		m.ups.Delete(id)
		return true
	})
}
