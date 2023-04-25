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

package config_applier

import (
	"net/url"
	"strconv"

	cerror "github.com/pingcap/tiflow/pkg/errors"
)

type Applier[T any] struct {
	ValueGetter       []ValueGetter[T]
	ValidateAndAdjust func(v T) (T, error)
	ValueHolder       *T
}

func (ap Applier[T]) Apply() error {
	var mergedValue T = *ap.ValueHolder
	for _, f := range ap.ValueGetter {
		override, v, err := f()
		if err != nil {
			return err
		}
		if !override {
			continue
		}
		nv := v
		if ap.ValidateAndAdjust != nil {
			nv, err = ap.ValidateAndAdjust(v)
			if err != nil {
				return err
			}
		}
		mergedValue = nv
	}
	*ap.ValueHolder = mergedValue
	return nil
}

type ValueGetter[T any] func() (bool, T, error)

func Apply(appliers []any) error {
	var err error
	for _, ap := range appliers {
		switch v := ap.(type) {
		case Applier[int]:
			if err = v.Apply(); err != nil {
				return err
			}
		case Applier[bool]:
			if err = v.Apply(); err != nil {
				return err
			}
		case Applier[string]:
			if err = v.Apply(); err != nil {
				return err
			}
		}
	}
	return nil
}

func UrlIntValGetter(values url.Values, key string) ValueGetter[int] {
	return func() (bool, int, error) {
		s := values.Get(key)
		if len(s) == 0 {
			return false, 0, nil
		}

		c, err := strconv.Atoi(s)
		if err != nil {
			return false, 0, cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
		}
		return true, c, nil
	}
}

func UrlBoolValGetter(values url.Values, key string) ValueGetter[bool] {
	return func() (bool, bool, error) {
		s := values.Get(key)
		if len(s) > 0 {
			enable, err := strconv.ParseBool(s)
			if err != nil {
				return false, false, cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
			}
			return true, enable, nil
		}
		return false, false, nil
	}
}

func UrlStringValGetter(values url.Values, key string) ValueGetter[string] {
	return func() (bool, string, error) {
		s := values.Get(key)
		return len(s) > 0, s, nil
	}
}

func ConfigFileIntValueGetter(c *int) ValueGetter[int] {
	return func() (bool, int, error) {
		if c == nil {
			return false, 0, nil
		}
		return true, *c, nil
	}
}

func ConfigFileBoolValueGetter(c *bool) ValueGetter[bool] {
	return func() (bool, bool, error) {
		if c == nil {
			return false, false, nil
		}
		return true, *c, nil
	}
}

func ConfigFileStringValueGetter(c *string) ValueGetter[string] {
	return func() (bool, string, error) {
		if c == nil {
			return false, "", nil
		}
		return true, *c, nil
	}
}
