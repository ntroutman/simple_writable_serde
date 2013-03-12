package com.nputmedia.hadoop.hive.simplwritableserde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
			out.writeDouble(val * 1.5d);

			out.writeBoolean(val % 2 == 0);

			Text.writeString(out, "txt" + val);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub

		}
	}

	public static class ListIntTestWritable implements Writable {
		private int val;

		public ListIntTestWritable(int val) {
			this.val = val;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			WritableUtils.writeVInt(out, val);
			for (int i = 0; i < val; i++) {
				out.writeInt(val);
			}
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			throw new RuntimeException("Not implemented yet.");
		}
	}

	public static class SimpleMapTestWritable implements Writable {
		private int val;

		public SimpleMapTestWritable(int val) {
			this.val = val;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			WritableUtils.writeVInt(out, val);
			for (int i = 0; i < val; i++) {
				Text.writeString(out, "key" + val + "_" + i);
				out.writeInt(val);
			}
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			throw new RuntimeException("Not implemented yet.");
		}
	}

	public static class SimpleStructTestWritable implements Writable {
		private int val;

		public SimpleStructTestWritable(int val) {
			this.val = val;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			WritableUtils.writeVInt(out, val);
			Text.writeString(out, "txt" + val);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			throw new RuntimeException("Not implemented yet.");
		}
	}

	public static class FullTestWritable implements Writable {
		private int val;

		public FullTestWritable(int val) {
			this.val = val;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// write out a couple columns
			Text.writeString(out, "col" + val);
			out.writeInt(val);

			// write out a list with simple structs as items
			WritableUtils.writeVInt(out, 5);
			for (int i = 0; i < 5; i++) {
				out.writeInt(val * i);
				out.writeInt(-val * i);
			}
			// write out a struct with a two primative columns and one map
			Text.writeString(out, "f1_" + val);
			out.writeDouble(val * 1.5d);
			// map<varint, struct<txt,int>>
			WritableUtils.writeVInt(out, 3);
			for (int i = 0; i < 3; i++) {
				// key
				WritableUtils.writeVInt(out, 100 + i);
				// value
				Text.writeString(out, "inner" + i);
				out.writeInt(i * i);
			}
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			throw new RuntimeException("Not implemented yet.");
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

		writer = createWriter(args[0], "list_int_test.seq", ListIntTestWritable.class);
		for (int i = 1; i <= 10; i++) {
			writer.append(NullWritable.get(), new ListIntTestWritable(i));
		}
		writer.close();

		writer = createWriter(args[0], "simple_map_test.seq", SimpleMapTestWritable.class);
		for (int i = 1; i <= 10; i++) {
			writer.append(NullWritable.get(), new SimpleMapTestWritable(i));
		}
		writer.close();

		writer = createWriter(args[0], "simple_struct_test.seq", SimpleStructTestWritable.class);
		for (int i = 1; i <= 10; i++) {
			writer.append(NullWritable.get(), new SimpleStructTestWritable(i));
		}
		writer.close();

		writer = createWriter(args[0], "full_test.seq", FullTestWritable.class);
		// for (int i = 1; i <= 10; i++) {
		// writer.append(NullWritable.get(), new FullTestWritable(i));
		// }
		writer.append(NullWritable.get(), new FullTestWritable(2));
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
