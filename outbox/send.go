package outbox

import (
	"context"
	"net/url"

	sq "github.com/elgris/sqrl"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"gopkg.daemonl.com/sqrlx"
)

type OutboxMessage interface {
	MessagingTopic() string
	MessagingHeaders() map[string]string
	proto.Message
}

type Sender interface {
	Send(ctx context.Context, tx sqrlx.Transaction, msg OutboxMessage) error
}

var DefaultSender Sender

func Send(ctx context.Context, tx sqrlx.Transaction, msg OutboxMessage) error {
	return DefaultSender.Send(ctx, tx, msg)
}

func init() {
	DefaultSender = &NamedSender{
		TableName:         "outbox",
		IDColumn:          "id",
		HeadersColumn:     "headers",
		DataColumn:        "message",
		DestinationColumn: "destination",
	}
}

type NamedSender struct {
	TableName         string
	IDColumn          string
	HeadersColumn     string
	DataColumn        string
	DestinationColumn string
}

func (ss *NamedSender) Send(ctx context.Context, tx sqrlx.Transaction, msg OutboxMessage) error {
	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	destination := msg.MessagingTopic()

	headers := &url.Values{}
	for k, v := range msg.MessagingHeaders() {
		headers.Add(k, v)
	}

	id := uuid.NewString()

	_, err = tx.Insert(ctx, sq.Insert(ss.TableName).
		Columns(ss.IDColumn, ss.DestinationColumn, ss.HeadersColumn, ss.DataColumn).
		Values(id, destination, headers.Encode(), msgBytes))

	return err
}
