// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Matt Jibson (mjibson@cockroachlabs.com)

package driver

import "fmt"

const (
	// Endpoint is the URL path prefix which accepts incoming
	// HTTP requests for the Redis API.
	Endpoint = "/redis/"
)

// GetUser implements security.RequestWithUser.
func (c Command) GetUser() string {
	return c.User
}

// Scan copies arguments into the values pointed at by dest.
func (c Command) Scan(dest ...*string) error {
	if len(dest) != len(c.Arguments) {
		return fmt.Errorf("wrong number of arguments for '%s' command", c.Command)
	}
	for i, d := range dest {
		*d = c.Arguments[i]
	}
	return nil
}

func (e Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Typ, e.Message)
}

func NewError(typ, message string) Error {
	return Error{
		Typ:     typ,
		Message: message,
	}
}
