package com.nputmedia.hadoop.hive.simplwritableserde;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This deserializer reads the entire underlying DataInputStream into a single
 * BytesWritable. It is intended for use with only SequenceFileReaders to get
 * access to the raw byte stream.
 * 
 * @author nathaniel
 * 
 */
public class BytesWritableSerialization extends Configured implements
		Serialization<Writable> {
	static class BytesWritableDeserializer extends Configured implements
			Deserializer<Writable> {

		private Class<?> writableClass;
		private DataInputStream dataIn;

		public BytesWritableDeserializer(Configuration conf, Class<?> c) {
			setConf(conf);
			this.writableClass = c;
		}

		public void open(InputStream in) {
			if (in instanceof DataInputStream) {
				dataIn = (DataInputStream) in;
			} else {
				dataIn = new DataInputStream(in);
			}
		}

		public Writable deserialize(Writable w) throws IOException {
			Writable writable;
			if (w == null) {
				writable = (Writable) ReflectionUtils.newInstance(
						writableClass, getConf());
			} else {
				writable = w;
			}
			writable.readFields(dataIn);
			return writable;
		}

		public void close() throws IOException {
			dataIn.close();
		}

	}

	static class BytesWritableSerializer implements Serializer<Writable> {

		private DataOutputStream dataOut;

		public void open(OutputStream out) {
			if (out instanceof DataOutputStream) {
				dataOut = (DataOutputStream) out;
			} else {
				dataOut = new DataOutputStream(out);
			}
		}

		public void serialize(Writable w) throws IOException {
			w.write(dataOut);
		}

		public void close() throws IOException {
			dataOut.close();
		}

	}

	public boolean accept(Class<?> c) {
		return Writable.class.isAssignableFrom(c);
	}

	public Deserializer<Writable> getDeserializer(Class<Writable> c) {
		return new BytesWritableDeserializer(getConf(), c);
	}

	public Serializer<Writable> getSerializer(Class<Writable> c) {
		return new BytesWritableSerializer();
	}
}
