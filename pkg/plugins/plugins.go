package plugins

import (
	"net"

	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
)

type Matcher struct {
	Matcher cmux.Matcher
	Serve   func(l net.Listener, db *client.DB) error
}
