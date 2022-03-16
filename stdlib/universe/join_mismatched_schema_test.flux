package universe_test


import "csv"
import "testing"
import "internal/debug"

a =
    csv.from(
        csv:
            "
#datatype,string,long,dateTime:RFC3339,double,string
#group,false,false,false,false,true
#default,_result,,,,
,result,table,_time,_value,key
,,0,2021-01-01T00:00:00Z,1.0,foo
,,0,2021-01-01T00:01:00Z,2.0,foo
#datatype,string,long,dateTime:RFC3339,double
#group,false,false,false,false
#default,_result,,,
,result,table,_time,_value
,,1,2021-01-01T00:00:00Z,1.5
,,1,2021-01-01T00:01:00Z,2.5
",
    )
b =
    csv.from(
        csv:
            "
#datatype,string,long,dateTime:RFC3339,double,string
#group,false,false,false,false,true
#default,_result,,,,
,result,table,_time,_value,key
,,0,2021-01-01T00:00:00Z,10.0,bar
,,0,2021-01-01T00:01:00Z,20.0,bar
",
    )

// No change in the result join schema on the fly as first table on each stream contains same columns.
// left stream's, second table is missing 'key' columns which will be filled by nil values in the result join schema column 'key_a'
testcase missing_column_on_left_stream_no_join_schema_change
{
        got =
            join(tables: {a, b}, on: ["_time"])
                |> debug.slurp()
        want =
            csv.from(
                csv:
                    "
#datatype,string,long,dateTime:RFC3339,double,double,string,string
#group,false,false,false,false,false,true,true
#default,_result,,,,,,
,result,table,_time,_value_a,_value_b,key_a,key_b
,,0,2021-01-01T00:00:00Z,1.0,10.0,foo,bar
,,0,2021-01-01T00:01:00Z,2.0,20.0,foo,bar
#datatype,string,long,dateTime:RFC3339,double,double,string,string
#group,false,false,false,false,false,false,true
#default,_result,,,,,,
,result,table,_time,_value_a,_value_b,key_a,key_b
,,1,2021-01-01T00:00:00Z,1.5,10.0,,bar
,,1,2021-01-01T00:01:00Z,2.5,20.0,,bar
",
            )

        testing.diff(got, want) |> yield()
    }

a1 =
    csv.from(
        csv:
            "
#datatype,string,long,dateTime:RFC3339,double
#group,false,false,false,false
#default,_result,,,
,result,table,_time,_value
,,0,2021-01-01T00:00:00Z,1.5
,,0,2021-01-01T00:01:00Z,2.5
#datatype,string,long,dateTime:RFC3339,double,string
#group,false,false,false,false,true
#default,_result,,,,
,result,table,_time,_value,key
,,1,2021-01-01T00:00:00Z,1.0,foo
,,1,2021-01-01T00:01:00Z,2.0,foo
",
    )
b1 =
    csv.from(
        csv:
            "
#datatype,string,long,dateTime:RFC3339,double,string
#group,false,false,false,false,true
#default,_result,,,,
,result,table,_time,_value,key
,,0,2021-01-01T00:00:00Z,10.0,bar
,,0,2021-01-01T00:01:00Z,20.0,bar
",
    )

// change in the result join schema on the fly as tables in left stream contains different schema
// left stream's, second table has extra column 'key'
testcase missing_column_on_left_stream_with_join_schema_change
{
        got =
            join(tables: {a1, b1}, on: ["_time"])
                |> debug.slurp()
        want =
            csv.from(
                csv:
                    "
#datatype,string,long,dateTime:RFC3339,double,double,string,string
#group,false,false,false,false,false,false,true
#default,_result,,,,,,
,result,table,_time,_value_a1,_value_b1,key_a1,key
,,0,2021-01-01T00:00:00Z,1.5,10.0,,bar
,,0,2021-01-01T00:01:00Z,2.5,20.0,,bar
#datatype,string,long,dateTime:RFC3339,double,double,string,string
#group,false,false,false,false,false,true,true
#default,_result,,,,,,
,result,table,_time,_value_a1,_value_b1,key_a1,key
,,1,2021-01-01T00:00:00Z,1.0,10.0,foo,bar
,,1,2021-01-01T00:01:00Z,2.0,20.0,foo,bar
",
            )

        testing.diff(got, want) |> yield()
    }

a2 =
    csv.from(
        csv:
            "
#datatype,string,long,dateTime:RFC3339,double,string
#group,false,false,false,false,true
#default,_result,,,,
,result,table,_time,_value,key
,,0,2021-01-01T00:00:00Z,1.0,foo
,,0,2021-01-01T00:01:00Z,2.0,foo
#datatype,string,long,dateTime:RFC3339,double
#group,false,false,false,false
#default,_result,,,
,result,table,_time,_value
,,1,2021-01-01T00:00:00Z,1.5
,,1,2021-01-01T00:01:00Z,2.5
",
    )
b2 =
    csv.from(
        csv:
            "
#datatype,string,long,dateTime:RFC3339,double,double
#group,false,false,false,false,true
#default,_result,,,,
,result,table,_time,_value,key
,,0,2021-01-01T00:00:00Z,10.0,8.0
,,0,2021-01-01T00:01:00Z,20.0,88.0
",
    )

// when a column exists on both sides but has a different type
// column 'key' is string on the left stream and double on the right stream
testcase same_column_on_both_stream_with_different_type
{
        got =
            join(tables: {a2, b2}, on: ["_time"])
                |> debug.slurp()
        want =
            csv.from(
                csv:
                    "
#datatype,string,long,dateTime:RFC3339,double,double,string,double
#group,false,false,false,false,false,true,true
#default,_result,,,,,,
,result,table,_time,_value_a2,_value_b2,key_a2,key_b2
,,0,2021-01-01T00:00:00Z,1.0,10.0,foo,8.0
,,0,2021-01-01T00:01:00Z,2.0,20.0,foo,8.0
#datatype,string,long,dateTime:RFC3339,double,double,string,double
#group,false,false,false,false,false,false,true
#default,_result,,,,,,
,result,table,_time,_value_a2,_value_b2,key_a2,key_b2
,,1,2021-01-01T00:00:00Z,1.5,10.0,,8.0
,,1,2021-01-01T00:01:00Z,2.5,20.0,,8.0
",
            )

        testing.diff(got, want) |> yield()
    }

a3 =
    csv.from(
        csv:
            "
#datatype,string,long,dateTime:RFC3339,double,string
#group,false,false,false,false,true
#default,_result,,,,
,result,table,_time,_value,key
,,0,2021-01-01T00:00:00Z,1.0,key0
,,0,2021-01-01T00:01:00Z,1.5,key0
#datatype,string,long,dateTime:RFC3339,double,string,string
#group,false,false,false,false,true,true
#default,_result,,,,,
,result,table,_time,_value,key,gkey_1
,,1,2021-01-01T00:00:00Z,2.0,key1,gkey1
,,1,2021-01-01T00:01:00Z,2.5,key1,gkey1
#datatype,string,long,dateTime:RFC3339,double,string,string
#group,false,false,false,false,true,true
#default,_result,,,,,
,result,table,_time,_value,key,gkey_2
,,2,2021-01-01T00:00:00Z,3.0,key2,gkey2
,,2,2021-01-01T00:01:00Z,3.5,key2,gkey2
",
    )
b3 =
    csv.from(
        csv:
            "
#datatype,string,long,dateTime:RFC3339,double,string
#group,false,false,false,false,true
#default,_result,,,,
,result,table,_time,_value,key
,,0,2021-01-01T00:00:00Z,10.0,key0
,,0,2021-01-01T00:01:00Z,10.5,key0
",
    )

// the group key is different on left and right stream
// Left Stream -                Right Stream -
// 0th table - key              0th table - key
// 1st table - key, gkey_1
// 2nd table - key, gkey_2
// Join on _time (non groupKey)
testcase different_group_key_on_left_and_right_stream_on_non_group_key
{
        got =
            join(tables: {a3, b3}, on: ["_time"])
                |> debug.slurp()
        want =
            csv.from(
                csv:
                    "
#datatype,string,long,dateTime:RFC3339,double,double,string,string,string,string
#group,false,false,false,false,false,true,true,false,false
#default,_result,,,,,,,,
,result,table,_time,_value_a3,_value_b3,key_a3,key_b3,gkey_1_a3,gkey_2_a3
,,0,2021-01-01T00:00:00Z,1.0,10.0,key0,key0,,
,,0,2021-01-01T00:01:00Z,1.5,10.5,key0,key0,,
#datatype,string,long,dateTime:RFC3339,double,double,string,string,string,string
#group,false,false,false,false,false,true,true,true,false
#default,_result,,,,,,,,
,result,table,_time,_value_a3,_value_b3,key_a3,key_b3,gkey_1_a3,gkey_2_a3
,,1,2021-01-01T00:00:00Z,2.0,10.0,key1,key0,gkey1,
,,1,2021-01-01T00:01:00Z,2.5,10.5,key1,key0,gkey1,
#datatype,string,long,dateTime:RFC3339,double,double,string,string,string,string
#group,false,false,false,false,false,true,true,false,true
#default,_result,,,,,,,,
,result,table,_time,_value_a3,_value_b3,key_a3,key_b3,gkey_1_a3,gkey_2_a3
,,2,2021-01-01T00:00:00Z,3.0,10.0,key2,key0,,gkey2
,,2,2021-01-01T00:01:00Z,3.5,10.5,key2,key0,,gkey2
",
            )

        testing.diff(got, want) |> yield()
    }

a4 =
    csv.from(
        csv:
            "
#datatype,string,long,double,string
#group,false,false,false,true
#default,_result,,,
,result,table,_value,key
,,0,1.0,key0

#datatype,string,long,double,string,string
#group,false,false,false,true,true
#default,_result,,,,
,result,table,_value,key,gkey_1
,,1,2.0,key1,gkey1

#datatype,string,long,double,string,string
#group,false,false,false,true,true
#default,_result,,,,
,result,table,_value,key,gkey_2
,,2,3.0,key2,gkey2
",
    )

b4 =
    csv.from(
        csv:
            "
#datatype,string,long,double,string
#group,false,false,false,true
#default,_result,,,
,result,table,_value,key
,,0,10.0,key0
,,0,20.0,key1
,,0,30.0,key2
",
    )

// the group key is different on left and right stream
// Left Stream -                Right Stream -
// 0th table - key              0th table - key
// 1st table - key, gkey_1
// 2nd table - key, gkey_2
// Join on key (groupKey)
testcase different_group_key_on_left_and_right_stream_on_group_key
{
        got =
            join(tables: {a4, b4}, on: ["key"])
                |> debug.slurp()

        want =
            csv.from(
                csv:
                    "
#datatype,string,long,double,double,string
#group,false,false,false,false,true
#default,_result,,,,
,result,table,_value_a4,_value_b4,key
,,0,1.0,10.0,key0

#datatype,string,long,double,double,string,string
#group,false,false,false,false,true,true
#default,_result,,,,,
,result,table,_value_a4,_value_b4,key,gkey_1
,,0,2.0,20.0,key1,gkey1

#datatype,string,long,double,double,string,string,string
#group,false,false,false,false,true,false,true
#default,_result,,,,,,
,result,table,_value_a4,_value_b4,key,gkey_1,gkey_2
,,0,3.0,30.0,key2,,gkey2
",
            )

        testing.diff(got, want) |> yield()
    }
