package com.nputmedia.hadoop.hive.simplwritableserde;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class RawBytesSequenceFileInputFormat extends
		SequenceFileInputFormat<Writable, Writable> {

}
