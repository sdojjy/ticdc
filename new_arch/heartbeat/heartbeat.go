// Copyright 2024 PingCAP, Inc.
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

package heartbeat

import (
	"context"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/p2p"
)

// 1. 组件启动的时候需要通知所有的 capture 一次 . 告诉子组件有新的 master 产生了， 让他们更改 master 地址 ，收集信息
// 2. heartbeat 是从 client 向 master 发送？
type AliveDetectionServer struct {
	// MessageServer and MessageRouter are for peer-messaging
	messageServer *p2p.MessageServer
	messageRouter p2p.MessageRouter

	SubComponents []Component
}

func NewAliveDetectionServer(ctx context.Context,
	messageServer *p2p.MessageServer,
	messageRouter p2p.MessageRouter) *AliveDetectionServer {
	return &AliveDetectionServer{
		messageServer: messageServer,
		messageRouter: messageRouter,
	}
}

type Component struct {
	// 组件的版本
	version int
	// 父组件的版本，收到消息后需要检查
	parentVersion int
	// 子组件
	subcomponent []Component

	// 组件的状态
	// starting, 正在启动，不能工作
	// running 启动完成对外服务
	// pending 和上级失联，不再工作
	// disconnected 和上级失联过长，主动退出
	state int
}

func (c *Component) Bootstrap() error {
	return nil
}

// 是否子组件，如果没有子组件是不需要通知所有结点的
func (c *Component) HasSubComponent() bool {
	return false
}

// 向所有的节点都发送一个 heartbeat 事件，收集所有的子组件
func (c *Component) Broadcast(capture []model.Capture) {
	for _, capture := range capture {
		// 发送信息到 capture
	}
}

func (c *Component) OnCaptureChanged() int {

}
