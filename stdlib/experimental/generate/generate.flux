package generate

// with is a function that generates data using with a schema.
//
// ## Parameters
// - `cardinality` is a record with a mapping of keys to the
//   desired cardinality for the key. The record must be of a form
//   where the value for each property is an int.
//
//   These keys will get incremented from zero to `n-1` and these
//   will get passed to the `key` function to generate the group key.
//
//   It is not necessary to include keys with a cardinality of one
//   in the cardinality list. An empty record will invoke the key function
//   exactly once.
//
// - `key` is a function that will return the group key for the given
//   schema. The schema is a record that matches with the cardinality
//   given and will take each key in the cardinality and include a value
//   from zero to `n-1` for that key. The produced group key will be used
//   to construct a table.
//
// - `values` is a function that produces a row for the given group key
//   at the given index. The output should include the `key` and this is
//   most easily done with `{key with ...}`.
//
// - `n` is the number of points that should be generated for each table.
//
// - `seed` will seed the random number generator. This allows the functions
//   in `experimental/rand` to be used with a custom and consistent seed.
//
//   If this is not specified, it will use the random number generator that
//   is seeded by the engine.
//
// ## Example
//
// ```
// import "experimental/rand"
// import "experimental/generate"
//
// generate.with(
//   cardinality: {t0: 10},
//   key: (schema) => ({
//     _measurement: "m0",
//     _field: "f0",
//     t0: "t${schema["t0"]}",
//   }),
//   values: (key, index) => ({key with
//     _value: rand.int(n: 100),
//   }),
//   n: 100,
// )
//
// ## Performance Testing
//
// This source can be useful for performance testing, but should only
// be used in the actual query that is being performance tested.
//
// To use this with performance testing is easiest when influxdb is used
// to store and read the generated data. You can use the example above and
// do this:
//
// ```
// |> to(bucket: "perftest")
// ```
//
// Then in a separate query, you can use `from(bucket: "perftest")` to read
// the data back.
//
// This source is not meant to be performant on its own.
// ```
builtin with : (
    cardinality: A,
    key: (schema: A) => B,
    values: (key: B, index: int) => C,
    n: int,
    ?seed: int,
) => [C] where A: Record, B: Record, C: Record
