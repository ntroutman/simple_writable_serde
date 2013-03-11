package com.nputmedia.hadoop.hive.simplwritableserde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class CreateSequenceFiles {
	public static class PrimativeTestWritable implements Writable {
		private int val;

		public PrimativeTestWritable(int val) {
			this.val = val;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			WritableUtils.writeVInt(out, val);
			
			out.writeShort(val);
			out.writeInt(val * 100);
			out.writeLong(val * 10000);

			out.writeFloat(val * 1.25f);
			out.writeDouble(val * 1.5);
			
			out.writeBoolean(val % 2 == 0);
			
			Text.writeString(out, "txt" + val);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub

		}

	}

	public static void main(String[] args) throws Exception {
		Writer writer;

		writer = createWriter(args[0], "long.seq", LongWritable.class);
		for (int i = 0; i < 10; i++) {
			writer.append(NullWritable.get(), new LongWritable(i));
		}
		writer.close();

		writer = createWriter(args[0], "text.seq", Text.class);
		for (int i = 0; i < 10; i++) {
			writer.append(NullWritable.get(), new Text("" + i));
		}
		writer.close();
		
		writer = createWriter(args[0], "primative_test.seq", PrimativeTestWritable.class);
		for (int i = 0; i < 10; i++) {
			writer.append(NullWritable.get(), new PrimativeTestWritable(i));
		}
		writer.close();
	}

	private static Writer createWriter(String parent, String file,
			Class<? extends Writable> valueClass) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Writer writer = SequenceFile.createWriter(fs, conf, new Path(parent, file),
				NullWritable.class, valueClass, CompressionType.NONE);
		return writer;
	}
}
