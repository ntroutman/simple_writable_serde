package com.nputmedia.hadoop.hive.simplwritableserde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Writable that uses varints
 * 
 * @author ntroutm
 * 
 */
public class VarIntWritable implements Writable {
	private int value;

	public VarIntWritable() {
		// do nothing
		this.value = 0;
	}

	public VarIntWritable(int value) {
		this.value = value;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, value);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		value = WritableUtils.readVInt(in);
	}
}
