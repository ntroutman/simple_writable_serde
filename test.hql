add jar target/simple-writable-serder-0.0.1-SNAPSHOT.jar
;

-------------------------
--	 primative_test
-------------------------

drop table if exists primative_test
;

create table primative_test (a_varint tinyint, a_short smallint, a_int int, a_long bigint, a_float float, a_double double, a_bool boolean, a_text string)
ROW FORMAT SERDE 'com.nputmedia.hadoop.hive.simplwritableserde.SimpleWritableSerde'
stored as 
INPUTFORMAT 'com.nputmedia.hadoop.hive.simplwritableserde.RawBytesSequenceFileInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
-- location '/user/ntroutm/primative_test'
;

load data local inpath 'src/test/resources/primative_test.seq' into table primative_test
;

select * from primative_test
;


-------------------------
--	 list_int_test
-------------------------

drop table if exists list_int_test
;

create table list_int_test (int_list array<int>)
ROW FORMAT SERDE 'com.nputmedia.hadoop.hive.simplwritableserde.SimpleWritableSerde'
stored as 
INPUTFORMAT 'com.nputmedia.hadoop.hive.simplwritableserde.RawBytesSequenceFileInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
;

load data local inpath 'src/test/resources/list_int_test.seq' into table list_int_test
;

select * from list_int_test
;

-------------------------
--	 simple_map_test
-------------------------

drop table if exists simple_map_test
;

create table simple_map_test (simple_map map<string, int>)
ROW FORMAT SERDE 'com.nputmedia.hadoop.hive.simplwritableserde.SimpleWritableSerde'
stored as 
INPUTFORMAT 'com.nputmedia.hadoop.hive.simplwritableserde.RawBytesSequenceFileInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
;

load data local inpath 'src/test/resources/simple_map_test.seq' into table simple_map_test
;

select * from simple_map_test
;

-------------------------
--	 simple_struct_test
-------------------------

drop table if exists simple_struct_test
;

create table simple_struct_test (simple_struct struct<x:tinyint, txt:string>)
ROW FORMAT SERDE 'com.nputmedia.hadoop.hive.simplwritableserde.SimpleWritableSerde'
stored as 
INPUTFORMAT 'com.nputmedia.hadoop.hive.simplwritableserde.RawBytesSequenceFileInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
;

load data local inpath 'src/test/resources/simple_struct_test.seq' into table simple_struct_test
;

select * from simple_struct_test
;

-------------------------
--	 full_test
-------------------------

drop table if exists full_test
;

create table full_test (
    rand_str string,
    pointless_int int,
    points array<struct<x:int, y:int>>,
    complicated struct<txt:string, half:double, mapit:map<tinyint, struct<foo:string, square:int>>>
)
ROW FORMAT SERDE 'com.nputmedia.hadoop.hive.simplwritableserde.SimpleWritableSerde'
stored as 
INPUTFORMAT 'com.nputmedia.hadoop.hive.simplwritableserde.RawBytesSequenceFileInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
;

load data local inpath 'src/test/resources/full_test.seq' into table full_test
;

select * from full_test
;