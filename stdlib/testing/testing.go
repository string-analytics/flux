package testing

import (
	"context"

	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/internal/errors"
	"github.com/influxdata/flux/runtime"
	"github.com/influxdata/flux/values"
)

func init() {
	runtime.RegisterPackageValue("testing", "do", values.NewFunction("do",
		runtime.MustLookupBuiltinType("testing", "do"),
		func(ctx context.Context, args values.Object) (values.Value, error) {
			return nil, errors.New(codes.Internal, "testing.do was called, did the testcase tranform fail to replace it?")
		},
		false,
	))

}
