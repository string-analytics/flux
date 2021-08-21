package rand

import (
	"context"
	"math/rand"

	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/internal/errors"
)

type key int

const randKey key = iota

func Seed(ctx context.Context, seed int64) context.Context {
	r := rand.New(rand.NewSource(seed))
	return context.WithValue(ctx, randKey, r)
}

func Get(ctx context.Context) (*rand.Rand, error) {
	r := ctx.Value(randKey)
	if r != nil {
		return r.(*rand.Rand), nil
	}
	return nil, errors.New(codes.Invalid, "random number generator has not been initialized")
}
