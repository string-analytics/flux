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
testcase normal
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
testcase normal
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
