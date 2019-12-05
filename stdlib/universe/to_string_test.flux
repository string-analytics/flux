package universe_test

import "testing"

option now = () => (2030-01-01T00:00:00Z)

inData = "
#datatype,string,long,dateTime:RFC3339,boolean,string,string
#group,false,false,false,false,true,true
#default,_result,,,,,
,result,table,_time,_value,_field,_measurement
,,0,2018-05-22T19:53:00Z,true,k0,m
,,0,2018-05-22T19:53:01Z,false,k0,m
#datatype,string,long,dateTime:RFC3339,double,string,string
#group,false,false,false,false,true,true
#default,_result,,,,,
,result,table,_time,_value,_field,_measurement
,,1,2018-05-22T19:53:10Z,0,k1,m
,,1,2018-05-22T19:53:11Z,1,k1,m
,,1,2018-05-22T19:53:12Z,1.0,k1,m
,,1,2018-05-22T19:53:13Z,110000,k1,m
,,1,2018-05-22T19:53:14Z,0.000011,k1,m
,,1,2018-05-22T19:53:15Z,0.0000024,k1,m
,,1,2018-05-22T19:53:16Z,-23.123456,k1,m
,,1,2018-05-22T19:53:17Z,-922337203.6854775808,k1,m
#datatype,string,long,dateTime:RFC3339,long,string,string
#group,false,false,false,false,true,true
#default,_result,,,,,
,result,table,_time,_value,_field,_measurement
,,2,2018-05-22T19:53:20Z,1,k2,m
,,2,2018-05-22T19:53:21Z,-1,k2,m
,,2,2018-05-22T19:53:22Z,0,k2,m
,,2,2018-05-22T19:53:23Z,2147483647,k2,m
,,2,2018-05-22T19:53:24Z,-2147483648,k2,m
,,2,2018-05-22T19:53:25Z,9223372036854775807,k2,m
,,2,2018-05-22T19:53:26Z,-9223372036854775808,k2,m
#datatype,string,long,dateTime:RFC3339,string,string,string
#group,false,false,false,false,true,true
#default,_result,,,,,
,result,table,_time,_value,_field,_measurement
,,3,2018-05-22T19:53:30Z,one,k3,m
,,3,2018-05-22T19:53:31Z,one's,k3,m
,,3,2018-05-22T19:53:32Z,one_two,k3,m
,,3,2018-05-22T19:53:33Z,one two,k3,m
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,string,string
#group,false,false,false,false,true,true
#default,_result,,,,,
,result,table,_time,_value,_field,_measurement
,,4,2018-05-22T19:53:40Z,2018-05-22T19:53:26Z,k4,m
,,4,2018-05-22T19:53:41Z,2018-05-22T19:53:26.033Z,k4,m
,,4,2018-05-22T19:53:42Z,2018-05-22T19:53:26.033066Z,k4,m
,,4,2018-05-22T19:53:43Z,2018-05-22T19:00:00+01:00,k4,m
#datatype,string,long,dateTime:RFC3339,unsignedLong,string,string
#group,false,false,false,false,true,true
#default,_result,,,,,
,result,table,_time,_value,_field,_measurement
,,5,2018-05-22T19:53:50Z,0,k5,m
,,5,2018-05-22T19:53:51Z,1,k5,m
,,5,2018-05-22T19:53:52Z,18446744073709551615,k5,m
,,5,2018-05-22T19:53:53Z,4294967296,k5,m
,,5,2018-05-22T19:53:54Z,9223372036854775808,k5,m
"

outData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,string
#group,false,false,true,true,false,true,true,false
#default,want,,,,,,,
,result,table,_start,_stop,_time,_field,_measurement,_value
,,0,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:00Z,k0,m,true
,,0,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:01Z,k0,m,false
,,1,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:10Z,k1,m,0
,,1,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:11Z,k1,m,1
,,1,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:12Z,k1,m,1
,,1,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:13Z,k1,m,110000
,,1,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:14Z,k1,m,0.000011
,,1,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:15Z,k1,m,0.0000024
,,1,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:16Z,k1,m,-23.123456
,,1,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:17Z,k1,m,-922337203.6854776
,,2,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:20Z,k2,m,1
,,2,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:21Z,k2,m,-1
,,2,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:22Z,k2,m,0
,,2,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:23Z,k2,m,2147483647
,,2,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:24Z,k2,m,-2147483648
,,2,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:25Z,k2,m,9223372036854775807
,,2,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:26Z,k2,m,-9223372036854775808
,,3,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:30Z,k3,m,one
,,3,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:31Z,k3,m,one's
,,3,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:32Z,k3,m,one_two
,,3,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:33Z,k3,m,one two
,,4,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:40Z,k4,m,2018-05-22T19:53:26.000000000Z
,,4,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:41Z,k4,m,2018-05-22T19:53:26.033000000Z
,,4,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:42Z,k4,m,2018-05-22T19:53:26.033066000Z
,,4,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:43Z,k4,m,2018-05-22T18:00:00.000000000Z
,,5,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:50Z,k5,m,0
,,5,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:51Z,k5,m,1
,,5,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:52Z,k5,m,18446744073709551615
,,5,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:53Z,k5,m,4294967296
,,5,2018-05-22T19:52:00Z,2030-01-01T00:00:00Z,2018-05-22T19:53:54Z,k5,m,9223372036854775808
"

t_to_string = (table=<-) =>
	(table
		|> range(start: 2018-05-22T19:52:00Z)
		|> toString())

test _to = () =>
	({input: testing.loadStorage(csv: inData), want: testing.loadMem(csv: outData), fn: t_to_string})

