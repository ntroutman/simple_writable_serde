add jar target/simple-writable-serder-0.0.1-SNAPSHOT.jar
;

drop table if exists primative_test
;

create external table primative_test (a_varint tinyint, a_short smallint, a_int int, a_long bigint, a_bool boolean, a_float float, a_double double, a_text string)
ROW FORMAT SERDE 'com.nputmedia.hadoop.hive.simplwritableserde.SimpleWritableSerde'
stored as 
--INPUTFORMAT 'org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat'
INPUTFORMAT 'com.nputmedia.hadoop.hive.simplwritableserde.SequenceFileAsBinaryInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
location '/user/nathaniel/primative_test'
;

select * from primative_test
;

