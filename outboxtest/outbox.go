package outboxtest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"

	sq "github.com/elgris/sqrl"
	"github.com/pentops/sqrlx.go/sqrlx"
	"google.golang.org/protobuf/proto"
)

type tb interface {
	Fatal(args ...any)
	Fatalf(format string, args ...any)
	Helper()
}

type OutboxAsserter struct {
	db *sqrlx.Wrapper

	TableName         string
	IDColumn          string
	HeadersColumn     string
	DataColumn        string
	DestinationColumn string
	ServiceNameHeader string
}

func NewOutboxAsserter(t tb, conn sqrlx.Connection) *OutboxAsserter {
	db, err := sqrlx.New(conn, sq.Dollar)
	if err != nil {
		t.Fatal(err.Error())
	}
	return &OutboxAsserter{
		db: db,

		TableName:         "outbox",
		IDColumn:          "id",
		HeadersColumn:     "headers",
		DataColumn:        "message",
		DestinationColumn: "destination",

		ServiceNameHeader: "grpc-service",
	}
}

type OutboxMessage interface {
	MessagingTopic() string
	MessagingHeaders() map[string]string
	proto.Message
}

func (oa *OutboxAsserter) PopMessage(tb TB, message OutboxMessage) {
	tb.Helper()

	destination := message.MessagingTopic()

	if err := oa.db.Transact(context.Background(), nil, func(ctx context.Context, tx sqrlx.Transaction) error {
		tb.Helper()
		var msgID string
		var msgHeader string
		var msgContent []byte

		if err := tx.SelectRow(
			ctx,
			sq.Select(oa.IDColumn, oa.HeadersColumn, oa.DataColumn).
				From(oa.TableName).
				Where(sq.Eq{oa.DestinationColumn: destination}).
				Limit(1),
		).Scan(&msgID, &msgHeader, &msgContent); errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("assertion failed, no outbox messages on %s for %T", destination, message)
		} else if err != nil {
			return err
		}

		storedHeaders, _ := url.ParseQuery(msgHeader)
		storedServiceHeader := storedHeaders.Get(oa.ServiceNameHeader)

		if provided := message.MessagingHeaders()[oa.ServiceNameHeader]; provided != storedServiceHeader {
			return fmt.Errorf("service name header (%s) should be %s but was %s", oa.ServiceNameHeader, provided, storedServiceHeader)
		}

		if err := proto.Unmarshal(msgContent, message); err != nil {
			return err
		}

		if _, err := tx.Delete(ctx, sq.Delete(oa.TableName).
			Where(sq.Eq{oa.IDColumn: msgID}),
		); err != nil {
			return err
		}
		return nil

	}); err != nil {
		tb.Fatalf(err.Error())
	}
}

type MessageMatch[M OutboxMessage] struct {
	Message    M
	conditions []func(M) bool
}

func NewMatcher[M OutboxMessage](message M, where ...func(M) bool) MessageMatch[M] {
	return MessageMatch[M]{
		Message:    message,
		conditions: where,
	}
}

func (m MessageMatch[M]) MessagingTopic() string {
	return m.Message.MessagingTopic()
}

func (m MessageMatch[M]) Attempt(serviceName string, data []byte) (bool, error) {
	if serviceName != m.Message.MessagingHeaders()["grpc-service"] {
		return false, nil
	}

	if err := proto.Unmarshal(data, m.Message); err != nil {
		return false, err
	}

	for _, condition := range m.conditions {
		if !condition(m.Message) {
			return false, nil
		}
	}

	return true, nil
}

type Matcher interface {
	MessagingTopic() string
	Attempt(serviceName string, data []byte) (bool, error)
}

func (oa *OutboxAsserter) PopMatching(tb TB, matcher Matcher) {
	tb.Helper()

	destination := matcher.MessagingTopic()

	if err := oa.db.Transact(context.Background(), nil, func(ctx context.Context, tx sqrlx.Transaction) error {
		tb.Helper()
		var msgID string
		var msgHeader string
		var msgContent []byte

		rows, err := tx.Select(
			ctx,
			sq.Select(oa.IDColumn, oa.HeadersColumn, oa.DataColumn).
				From(oa.TableName).
				Where(sq.Eq{oa.DestinationColumn: destination}),
		)
		if err != nil {
			return err
		}

		defer rows.Close()

		var foundOne string
		for rows.Next() {
			err := rows.Scan(&msgID, &msgHeader, &msgContent)
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("assertion failed, no outbox messages on %s", destination)
			} else if err != nil {
				return err
			}

			storedHeaders, _ := url.ParseQuery(msgHeader)
			storedServiceHeader := storedHeaders.Get(oa.ServiceNameHeader)
			didHandle, err := matcher.Attempt(storedServiceHeader, msgContent)
			if err != nil {
				return err
			}
			if !didHandle {
				continue
			}

			foundOne = msgID

			break
		}

		for rows.Next() {
		}

		if foundOne == "" {
			return fmt.Errorf("no messages matched for %s with custom matcher", destination)
		}

		if _, err := tx.Delete(ctx, sq.Delete(oa.TableName).
			Where(sq.Eq{oa.IDColumn: foundOne}),
		); err != nil {
			return err
		}

		return nil

	}); err != nil {
		tb.Fatalf(err.Error())
	}
}

func (oa *OutboxAsserter) ForEachMessage(tb TB, callback func(string, string, []byte)) {
	tb.Helper()
	type msgRow struct {
		Destination string
		Headers     string
		Data        []byte
	}

	messageRows := []msgRow{}
	if txErr := oa.db.Transact(context.Background(), nil, func(contextVal context.Context, tx sqrlx.Transaction) error {
		tb.Helper()
		dataRows, err := tx.Select(contextVal, sq.Select(
			oa.DestinationColumn,
			oa.HeadersColumn,
			oa.DataColumn,
		).From(oa.TableName))
		if err != nil {
			return err
		}
		defer dataRows.Close()
		for dataRows.Next() {
			msgRow := msgRow{}
			if scanErr := dataRows.Scan(&msgRow.Destination, &msgRow.Headers, &msgRow.Data); scanErr != nil {
				return scanErr
			}
			messageRows = append(messageRows, msgRow)

		}
		return nil
	}); txErr != nil {
		tb.Fatal(txErr.Error())
	}

	for _, msgRow := range messageRows {
		storedHeaders, _ := url.ParseQuery(msgRow.Headers)
		storedServiceHeader := storedHeaders.Get(oa.ServiceNameHeader)
		callback(msgRow.Destination, storedServiceHeader, msgRow.Data)
	}
}

func (oa *OutboxAsserter) AssertNoMessages(tb TB) {
	tb.Helper()
	msgCounts := []string{}
	if txErr := oa.db.Transact(context.Background(), nil, func(contextVal context.Context, tx sqrlx.Transaction) error {
		tb.Helper()
		dataRows, err := tx.Select(contextVal, sq.Select(
			oa.DestinationColumn,
			"count(*)",
		).
			From(oa.TableName).
			GroupBy(oa.DestinationColumn).
			Having("count(*) > 0"))
		if err != nil {
			return err
		}
		defer dataRows.Close()
		for dataRows.Next() {
			var dest string
			var msgCount uint64
			if scanErr := dataRows.Scan(&dest, &msgCount); scanErr != nil {
				return scanErr
			}
			msgCounts = append(msgCounts, fmt.Sprintf("%d messages in %s", msgCount, dest))
		}
		return nil
	}); txErr != nil {
		tb.Fatal(txErr.Error())
	}
	if len(msgCounts) != 0 {
		tb.Fatalf("No messages expected, but found: %s", strings.Join(msgCounts, ", "))
	}
}

func (oa *OutboxAsserter) AssertTopicIsEmpty(tb testing.TB, topic string) {
	tb.Helper()
	var msgCount uint64
	if txErr := oa.db.Transact(context.Background(), nil, func(contextVal context.Context, tx sqrlx.Transaction) error {
		return tx.SelectRow(contextVal, sq.
			Select("count(*)").
			From(oa.TableName).
			Where(sq.Eq{oa.DestinationColumn: topic})).
			Scan(&msgCount)
	}); txErr != nil {
		tb.Fatal(txErr.Error())
	}
	if msgCount != 0 {
		tb.Fatalf("No messages expected, but found %d", msgCount)
	}
}

func (oa *OutboxAsserter) PurgeAll(tb TB) {
	tb.Helper()
	if txErr := oa.db.Transact(context.Background(), nil, func(contextVal context.Context, tx sqrlx.Transaction) error {
		_, delErr := tx.Delete(contextVal, sq.Delete(oa.TableName))
		return delErr
	}); txErr != nil {
		tb.Fatalf("Transaction Error %s", txErr.Error())
	}
}
