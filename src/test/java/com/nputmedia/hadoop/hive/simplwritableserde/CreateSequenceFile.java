package com.nputmedia.hadoop.hive.simplwritableserde;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;

public class CreateSequenceFile {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Writer writer = SequenceFile.createWriter(fs, conf, new Path(args[0], "long.seq"),
				NullWritable.class, LongWritable.class);
		for (int i = 0; i < 10; i++) {
			writer.append(NullWritable.get(), new LongWritable(i));
		}
		writer.close();
	}
}
