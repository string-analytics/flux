package rand

import (
	"context"

	"github.com/influxdata/flux/dependencies/rand"
	"github.com/influxdata/flux/interpreter"
	"github.com/influxdata/flux/runtime"
	"github.com/influxdata/flux/values"
)

const pkgpath = "experimental/rand"

func init() {
	runtime.RegisterPackageValue(pkgpath, "int", values.NewFunction(
		"int",
		runtime.MustLookupBuiltinType(pkgpath, "int"),
		func(ctx context.Context, args values.Object) (values.Value, error) {
			return interpreter.DoFunctionCallContext(Int, ctx, args)
		},
		false,
	))
}

func Int(ctx context.Context, args interpreter.Arguments) (values.Value, error) {
	r, err := rand.Get(ctx)
	if err != nil {
		return nil, err
	}

	n, ok, err := args.GetInt("n")
	if err != nil {
		return nil, err
	}

	v := func(n int64, ok bool) int64 {
		if !ok {
			return r.Int63()
		}
		return r.Int63n(n)
	}(n, ok)
	return values.NewInt(v), nil
}
