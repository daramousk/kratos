package test

import (
	"context"
	"github.com/bxcodec/faker/v3"
	"github.com/gofrs/uuid"
	"github.com/ory/kratos/persistence"
	"github.com/ory/kratos/selfservice/flow"
	"github.com/ory/kratos/selfservice/flow/login"
	"github.com/ory/kratos/ui/container"
	"github.com/ory/kratos/x"
	"github.com/ory/x/assertx"
	"github.com/ory/x/sqlcon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFlowPersister(ctx context.Context, p persistence.Persister) func(t *testing.T) {
	var clearids = func(r *login.Flow) {
		r.ID = uuid.UUID{}
	}

	return func(t *testing.T) {
		t.Run("case=should error when the login flow does not exist", func(t *testing.T) {
			_, err := p.GetLoginFlow(ctx, x.NewUUID())
			require.Error(t, err)
		})

		var newFlow = func(t *testing.T) *login.Flow {
			var r login.Flow
			require.NoError(t, faker.FakeData(&r))
			clearids(&r)
			return &r
		}

		t.Run("case=should create with set ids", func(t *testing.T) {
			var r login.Flow
			require.NoError(t, faker.FakeData(&r))
			require.NoError(t, p.CreateLoginFlow(ctx, &r))
		})

		t.Run("case=should create a new login flow and properly set IDs", func(t *testing.T) {
			r := newFlow(t)
			err := p.CreateLoginFlow(ctx, r)
			require.NoError(t, err, "%#v", err)
			assert.NotEqual(t, uuid.Nil, r.ID)
		})

		t.Run("case=should create and fetch a login flow", func(t *testing.T) {
			expected := newFlow(t)
			err := p.CreateLoginFlow(ctx, expected)
			require.NoError(t, err)

			actual, err := p.GetLoginFlow(ctx, expected.ID)
			require.NoError(t, err)

			assert.EqualValues(t, expected.ID, actual.ID)
			x.AssertEqualTime(t, expected.IssuedAt, actual.IssuedAt)
			x.AssertEqualTime(t, expected.ExpiresAt, actual.ExpiresAt)
			assert.EqualValues(t, expected.RequestURL, actual.RequestURL)
			assert.EqualValues(t, expected.Active, actual.Active)
			assertx.EqualAsJSON(t, expected.UI, actual.UI, "expected:\t%s\nactual:\t%s", expected.UI, actual.UI)
		})

		t.Run("case=should properly set the flow type", func(t *testing.T) {
			expected := newFlow(t)
			expected.Forced = true
			expected.Type = flow.TypeAPI
			expected.UI = container.New("ory-sh")

			err := p.CreateLoginFlow(ctx, expected)
			require.NoError(t, err)

			actual, err := p.GetLoginFlow(ctx, expected.ID)
			require.NoError(t, err)
			assert.Equal(t, flow.TypeAPI, actual.Type)

			actual.UI = container.New("not-ory-sh")
			actual.Type = flow.TypeBrowser
			actual.Forced = true

			require.NoError(t, p.UpdateLoginFlow(ctx, actual))

			actual, err = p.GetLoginFlow(ctx, actual.ID)
			require.NoError(t, err)
			assert.Equal(t, flow.TypeBrowser, actual.Type)
			assert.True(t, actual.Forced)
			assert.Equal(t, "not-ory-sh", actual.UI.Action)
		})

		t.Run("case=should not cause data loss when updating a request without changes", func(t *testing.T) {
			expected := newFlow(t)
			err := p.CreateLoginFlow(ctx, expected)
			require.NoError(t, err)

			actual, err := p.GetLoginFlow(ctx, expected.ID)
			require.NoError(t, err)

			require.NoError(t, p.UpdateLoginFlow(ctx, actual))

			actual, err = p.GetLoginFlow(ctx, expected.ID)
			require.NoError(t, err)
			assertx.EqualAsJSON(t, expected.UI, actual.UI)
		})

		t.Run("case=network", func(t *testing.T) {
			id := x.NewUUID()
			nid := x.NewUUID()
			p := p.WithNetworkID(nid)

			t.Run("sets id on creation", func(t *testing.T) {
				expected := &login.Flow{ID: id}
				require.NoError(t, p.CreateLoginFlow(ctx, expected))
				assert.EqualValues(t, id, expected.ID)
				assert.EqualValues(t, nid, expected.NID)

				actual, err := p.GetLoginFlow(ctx, id)
				require.NoError(t, err)
				assert.EqualValues(t, id, actual.ID)
				assert.EqualValues(t, nid, actual.NID)
			})

			t.Run("can not update id", func(t *testing.T) {
				expected, err := p.GetLoginFlow(ctx, id)
				require.NoError(t, err)
				require.NoError(t, p.UpdateLoginFlow(ctx, expected))

				actual, err := p.GetLoginFlow(ctx, id)
				require.NoError(t, err)
				assert.EqualValues(t, id, actual.ID)
				assert.EqualValues(t, nid, actual.NID)
			})

			t.Run("can not update on another network", func(t *testing.T) {
				expected, err := p.GetLoginFlow(ctx, id)
				require.NoError(t, err)

				other := x.NewUUID()
				p := p.WithNetworkID(other)

				expected.RequestURL = "updated"
				require.Error(t, p.UpdateLoginFlow(ctx, expected))

				actual, err := p.GetLoginFlow(ctx, id)
				require.ErrorIs(t, err, sqlcon.ErrNoRows)

				p = p.WithNetworkID(nid)
				actual, err = p.GetLoginFlow(ctx, id)
				require.NoError(t, err)
				require.NotEqual(t, "updated", actual.RequestURL)
			})

			t.Run("can not get on another network", func(t *testing.T) {
				p := p.WithNetworkID( x.NewUUID())
				_, err := p.GetLoginFlow(ctx, id)
				require.ErrorIs(t, err, sqlcon.ErrNoRows)
			})
		})
	}
}
