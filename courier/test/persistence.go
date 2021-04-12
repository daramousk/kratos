package test

import (
	"context"
	"fmt"
	"github.com/bxcodec/faker/v3"
	"github.com/ory/kratos/courier"
	"github.com/ory/kratos/persistence"
	"github.com/ory/kratos/x"
	"github.com/ory/x/sqlcon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestPersister(ctx context.Context, p persistence.Persister) func(t *testing.T) {
	return func(t *testing.T) {
		nid := x.NewUUID()
		p := p.WithNetworkID(nid)

		t.Run("case=no messages in queue", func(t *testing.T) {
			m, err := p.NextMessages(ctx, 10)
			require.EqualError(t, err, courier.ErrQueueEmpty.Error())
			assert.Len(t, m, 0)

			_, err = p.LatestQueuedMessage(ctx)
			require.EqualError(t, err, courier.ErrQueueEmpty.Error())
		})

		messages := make([]courier.Message, 5)
		t.Run("case=add messages to the queue", func(t *testing.T) {
			for k := range messages {
				require.NoError(t, faker.FakeData(&messages[k]))
				require.NoError(t, p.AddMessage(ctx, &messages[k]))
				time.Sleep(time.Second) // wait a bit so that the timestamp ordering works in MySQL.
			}
		})

		t.Run("case=latest message in queue", func(t *testing.T) {
			expected, err := p.LatestQueuedMessage(ctx)
			require.NoError(t, err)

			actual := messages[len(messages)-1]
			assert.Equal(t, expected.ID, actual.ID)
			assert.Equal(t, expected.Subject, actual.Subject)
		})

		t.Run("case=pull messages from the queue", func(t *testing.T) {
			for k, expected := range messages {
				expected.Status = courier.MessageStatusProcessing
				t.Run(fmt.Sprintf("message=%d", k), func(t *testing.T) {
					messages, err := p.NextMessages(ctx, 1)
					require.NoError(t, err)
					require.Len(t, messages, 1)

					actual := messages[0]
					assert.Equal(t, expected.ID, actual.ID)
					assert.Equal(t, expected.Subject, actual.Subject)
					assert.Equal(t, expected.Body, actual.Body)
					assert.Equal(t, expected.Status, actual.Status)
					assert.Equal(t, expected.Type, actual.Type)
					assert.Equal(t, expected.Recipient, actual.Recipient)
				})
			}

			_, err := p.NextMessages(ctx, 10)
			require.EqualError(t, err, courier.ErrQueueEmpty.Error())
		})

		t.Run("case=setting message status", func(t *testing.T) {
			require.NoError(t, p.SetMessageStatus(ctx, messages[0].ID, courier.MessageStatusQueued))
			ms, err := p.NextMessages(ctx, 1)
			require.NoError(t, err)
			require.Len(t, ms, 1)
			assert.Equal(t, messages[0].ID, ms[0].ID)

			require.NoError(t, p.SetMessageStatus(ctx, messages[0].ID, courier.MessageStatusSent))
			_, err = p.NextMessages(ctx, 1)
			require.EqualError(t, err, courier.ErrQueueEmpty.Error())
		})

		t.Run("case=network", func(t *testing.T) {
			id := x.NewUUID()

			t.Run("sets id on creation", func(t *testing.T) {
				expected := courier.Message{ID: id}
				require.NoError(t, p.AddMessage(ctx, &expected))

				assert.EqualValues(t, id, expected.ID)
				assert.EqualValues(t, nid, expected.NID)

				actual, err := p.GetContinuitySession(ctx, id)
				require.NoError(t, err)
				assert.EqualValues(t, id, actual.ID)
				assert.EqualValues(t, nid, actual.NID)
			})

			t.Run("can not get on another network", func(t *testing.T) {
				p := p.WithNetworkID(x.NewUUID())
				_, err := p.GetLoginFlow(ctx, id)
				require.ErrorIs(t, err, sqlcon.ErrNoRows)
			})

			t.Run("can not delete on another network", func(t *testing.T) {
				p := p.WithNetworkID(x.NewUUID())
				err := p.DeleteContinuitySession(ctx, id)
				require.ErrorIs(t, err, sqlcon.ErrNoRows)
			})
		})
	}
}
