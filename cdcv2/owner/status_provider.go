// Copyright 2023 PingCAP, Inc.
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

package owner

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// NewStatusProvider returns a new StatusProvider for the owner.
func NewStatusProvider(owner *OwnerImpl) owner.StatusProvider {
	return &ownerStatusProvider{owner: owner}
}

type ownerStatusProvider struct {
	owner *OwnerImpl
}

func (p *ownerStatusProvider) GetAllChangeFeedStatuses(ctx context.Context) (
	map[model.ChangeFeedID]*model.ChangeFeedStatusForAPI, error,
) {
	query := &owner.Query{
		Tp: owner.QueryAllChangeFeedStatuses,
	}
	if err := p.sendQueryToOwner(ctx, query); err != nil {
		return nil, errors.Trace(err)
	}
	return query.Data.(map[model.ChangeFeedID]*model.ChangeFeedStatusForAPI), nil
}

func (p *ownerStatusProvider) GetChangeFeedStatus(ctx context.Context,
	changefeedID model.ChangeFeedID,
) (*model.ChangeFeedStatusForAPI, error) {
	statuses, err := p.GetAllChangeFeedStatuses(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	status, exist := statuses[changefeedID]
	if !exist {
		return nil, cerror.ErrChangeFeedNotExists.GenWithStackByArgs(changefeedID)
	}
	return status, nil
}

func (p *ownerStatusProvider) GetAllChangeFeedInfo(ctx context.Context) (
	map[model.ChangeFeedID]*model.ChangeFeedInfo, error,
) {
	query := &owner.Query{
		Tp: owner.QueryAllChangeFeedInfo,
	}
	if err := p.sendQueryToOwner(ctx, query); err != nil {
		return nil, errors.Trace(err)
	}
	return query.Data.(map[model.ChangeFeedID]*model.ChangeFeedInfo), nil
}

func (p *ownerStatusProvider) GetChangeFeedInfo(ctx context.Context,
	changefeedID model.ChangeFeedID,
) (*model.ChangeFeedInfo, error) {
	infos, err := p.GetAllChangeFeedInfo(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	info, exist := infos[changefeedID]
	if !exist {
		return nil, cerror.ErrChangeFeedNotExists.GenWithStackByArgs(changefeedID)
	}
	return info, nil
}

func (p *ownerStatusProvider) GetAllTaskStatuses(ctx context.Context,
	changefeedID model.ChangeFeedID,
) (map[model.CaptureID]*model.TaskStatus, error) {
	query := &owner.Query{
		Tp:           owner.QueryAllTaskStatuses,
		ChangeFeedID: changefeedID,
	}
	if err := p.sendQueryToOwner(ctx, query); err != nil {
		return nil, errors.Trace(err)
	}
	return query.Data.(map[model.CaptureID]*model.TaskStatus), nil
}

func (p *ownerStatusProvider) GetProcessors(ctx context.Context) ([]*model.ProcInfoSnap, error) {
	query := &owner.Query{
		Tp: owner.QueryProcessors,
	}
	if err := p.sendQueryToOwner(ctx, query); err != nil {
		return nil, errors.Trace(err)
	}
	return query.Data.([]*model.ProcInfoSnap), nil
}

func (p *ownerStatusProvider) GetCaptures(ctx context.Context) ([]*model.CaptureInfo, error) {
	query := &owner.Query{
		Tp: owner.QueryCaptures,
	}
	if err := p.sendQueryToOwner(ctx, query); err != nil {
		return nil, errors.Trace(err)
	}
	return query.Data.([]*model.CaptureInfo), nil
}

func (p *ownerStatusProvider) IsHealthy(ctx context.Context) (bool, error) {
	query := &owner.Query{
		Tp: owner.QueryHealth,
	}
	if err := p.sendQueryToOwner(ctx, query); err != nil {
		return false, errors.Trace(err)
	}
	return query.Data.(bool), nil
}

func (p *ownerStatusProvider) IsChangefeedOwner(ctx context.Context, id model.ChangeFeedID) (bool, error) {
	query := &owner.Query{
		Tp:           owner.QueryOwner,
		ChangeFeedID: id,
	}
	if err := p.sendQueryToOwner(ctx, query); err != nil {
		return false, errors.Trace(err)
	}
	return query.Data.(bool), nil
}

func (p *ownerStatusProvider) sendQueryToOwner(ctx context.Context, query *owner.Query) error {
	doneCh := make(chan error, 1)
	p.owner.Query(query, doneCh)

	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case err := <-doneCh:
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}
