# Polymorphic record labels

## Motivation

We have many operations that accept a column name as a parameter and uses that to operate on that specific record field (and in some cases multiple fields).


```flux
builtin fill : (<-tables: stream[A], ?column: string, ?value: B, ?usePrevious: bool) => stream[C]
    where
    A: Record,
    C: Record
```

However as we can see from `fill`'s signature, the type system is wholly unaware of whether the incoming record (`A`) has the `column`. It also doesn't know what the shape of the returned record (`C`) is either. To fix this issue we need a way to connect the `column` value with the input (and output records). As a solution I propose we add "polymorphic record labels" which would let us define `fill` as follows.

```flux
builtin fill : (<-tables: stream[{ A with 'column: B }], ?column: 'column, ?value: B, ?usePrevious: bool) => stream[{ A with 'column: B }]
    where A: Record
```

`'column` in this case indicates a polymorphic label which would allow `fill` to be called as normal, however when passing a "label" (a string) that is known at compile time it lets us enforce the typing of the input and output records.

```flux

// 'column is inferred to `a` which propagates to the input and output records, making them `{ A with a: B }`
[{ a: 1 }] |> fill(column: "a", value: 0)

// ERROR: the input record lacks the field `a`
[{}] |> fill(column: "a", value: 0)
```


## Explanation

Polymorphic labels adds the ability to define record types where the field names (labels) can be a variable instead of a literal name in a similar way that type variables can exist in a type.

```
{ 'col: string } vs { some_column: string }
(x: A) => A vs (x: string) => string
```

The types that fit into a record field implement the new `Label` kind to ensure that unexpected types like `Int` are an error when used in a record field.

To make the change as transparent as possible string literals now get the new `Label("literal")` type instead of `String` making `"abc": Literal("abc")`. While this means that two different string literals will have different types we must still treat `Label` types as strings in most cases. Consider

```flux
if b then "a" else "b"
```

The branches will have the `Label("a")` and `Label("b")` respectively, which we still need to unify successfully and the resulting type of the expression should still be a `string`. As long as we limit  the uses of label types to direct uses in calls (like in `fill` above). We may keep the same type checking behaviour by treating `Label` as a `String` types in every instance except when type checking function calls, in which case we specialize unification to a subsumption check where we allow some limited sub-typing.

```flux
// The original fill call is allowed such that `'column` is inferred to be `Label("a")`
fill(column: "a", value: 0)

// We should (probably) still allow dynamic strings for backwards compatibility sake so the `string` type
// also implements the `Label` Kind
c = "a" + ""
fill(column: c, value: 0)

builtin add : (x: A, y: A) => A where A: Addable

// This must still be allowed, however a naive implementation would first infer `A <=> Label("a")`
// and then fail to unify `Label("a") <=> Label("b")`. If we keep treating `Label(..)` as a string except in cases where it unifies to a variable that actually has the `Label` type.
add(x: "a", y: "b")

builtin func : (opt: string) => int

// This will work since `Label("a")` is a sub type of `string`
func(opt: "a")

// Possible extension where we only allow some specific labels to be passed in
builtin func : (opt: "option1" | "option2") => int

// "option1" is an allowed option
func(opt: "option1")
// However "option3" is not allowed
func(opt: "option3")
// However passing in a dynamic string could be disallowed (`string` is to general)
o = "option" + ""
func(opt: o)
```

Row polymorphism introduces another wrinkle to the implementation. When fields aren't necessarily know at the point that `unify` are called we aren't able to match the field of either side.

```flux
// Unifying know fields works regardless of the order that they are defined in
{ a: int, b: string } <=> { b: string, a: int }

// Should 'column unify against `a` or `b` field on the left side?
{ 'column: A } <=> { b: string, a: int }
```

There may be a consistent way to unify these records in the face of type variables, however an easy workaround would be to delay the unification of records with unknown fields until they have been resolved, at which point they can unify normally. If there is a field that is still unknown when type checking is done we can designate that as an error.

```flux
builtin badFill : (<-tables: stream[{ A with 'column: B }], ?value: B) => stream[{ A with 'column: B }]
    where A: Record

// There is no way to determine what `'column` should be, so we must error
badFill()
```
