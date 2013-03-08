package com.nputmedia.hadoop.hive.simplwritableserde;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.Lists;

/**
 * To be used with: org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat
 * 
 * @author ntroutman
 * 
 */
public class SimpleWritableSerde implements Deserializer {

	private TypeInfo rowTypeInfo;
	private StructObjectInspector rowObjectInspector;
	private ArrayList<Writable> row;

	@Override
	public void initialize(Configuration conf, Properties tbl) throws SerDeException {
		// Get column names which are supplied by hive in the tables properties
		String columnNameProperty = tbl.getProperty("columns");
		String columnTypeProperty = tbl.getProperty("columns.types");

		// column names are comma separated
		ArrayList<String> columnNames;
		if (columnNameProperty.length() == 0) {
			columnNames = Lists.newArrayList();
		} else {
			columnNames = (ArrayList<String>) Arrays.asList(columnNameProperty.split(","));
		}

		// get the column types using the utility functions
		ArrayList<TypeInfo> columnTypes;
		if (columnTypeProperty.length() == 0) {
			columnTypes = Lists.newArrayList();
		} else {
			columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
		}
		assert (columnNames.size() == columnTypes.size());

		// Create row struct type info and object inspector from the column
		// names and types
		rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
		rowObjectInspector = (StructObjectInspector) TypeInfoUtils
				.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);

		// Create an array to hold the values for each row and populate each
		// column with the appropriate writable
		row = Lists.newArrayListWithCapacity(columnNames.size());
		for (int i = 0; i < columnNames.size(); i++) {
			row.add(getWritable(columnTypes.get(i)));
		}
	}

	@Override
	public Object deserialize(Writable blob) throws SerDeException {
		byte[] bytes = ((BytesWritable) blob).getBytes();
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
		for (int i = 0; i < row.size(); i++) {
			try {
				row.get(i).readFields(in);
			} catch (IOException e) {
				throw new SerDeException(e);
			}
		}
		return row;
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return rowObjectInspector;
	}

	@Override
	public SerDeStats getSerDeStats() {
		return null;
	}

	private Writable getWritable(TypeInfo type) throws SerDeException {
		switch (type.getCategory()) {
		case PRIMITIVE: {
			PrimitiveTypeInfo ptype = (PrimitiveTypeInfo) type;
			
			switch (ptype.getPrimitiveCategory()) {
			case VOID: {
				return null;
			}
			case BOOLEAN: {
				return new BooleanWritable();
			}
			case BYTE: {
				return new VarIntWritable();
			}
			case SHORT: {
				return new ShortWritable();
			}
			case INT: {
				return new IntWritable();
			}
			case LONG: {
				return new LongWritable();
			}
			case FLOAT: {
				return new FloatWritable();
			}
			case DOUBLE: {
				return new DoubleWritable();
			}
			case STRING: {
				return new Text();
			}
			default: {
				throw new RuntimeException("Unrecognized type: " + ptype.getPrimitiveCategory());
			}
			}
		}
		case LIST:
		case MAP:
		case STRUCT: {
			throw new SerDeException("List, Map and Struct not supported by SimpleWritable");
		}
		default: {
			throw new RuntimeException("Unrecognized type: " + type.getCategory());
		}
		}
	}
}
