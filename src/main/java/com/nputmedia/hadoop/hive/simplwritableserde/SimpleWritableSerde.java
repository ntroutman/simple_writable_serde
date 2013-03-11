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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * To be used with:
 * org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat
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
		List<String> columnNames;
		if (columnNameProperty.length() == 0) {
			columnNames = Lists.newArrayList();
		} else {
			columnNames = Lists.newArrayList(columnNameProperty.split(","));
		}

		// the column types to return to hive that represent the table, not the
		// underlying data
		ArrayList<TypeInfo> tableColumnTypes;
		
		// an array to hold the values for each row and populate each
		// column with the appropriate writable
		row = Lists.newArrayListWithCapacity(columnNames.size());
		if (columnTypeProperty.length() == 0) {
			tableColumnTypes = Lists.newArrayList();
		} else {
			List<TypeInfo> rawColumnTypes = TypeInfoUtils
					.getTypeInfosFromTypeString(columnTypeProperty);
			tableColumnTypes = Lists.newArrayListWithCapacity(rawColumnTypes.size());
			for (int i = 0; i < rawColumnTypes.size(); i++) {
				TypeInfo rawColumn = rawColumnTypes.get(i);
				// bytes,aka tinyint, are actually treated as var ints
				if (isByte(rawColumn)) {
					tableColumnTypes.add(TypeInfoFactory.intTypeInfo);
					row.add(new VarIntWritable());
				} else {
					tableColumnTypes.add(rawColumn);
					row.add(getWritable(rawColumn));
				}
			}
		}
		assert (columnNames.size() == tableColumnTypes.size());

		// Create row struct type info and object inspector from the column
		// names and types
		rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, tableColumnTypes);
		rowObjectInspector = (StructObjectInspector) TypeInfoUtils
				.getStandardWritableObjectInspectorFromTypeInfo(rowTypeInfo);

	}

	private boolean isByte(TypeInfo type) {
		if (type.getCategory() == Category.PRIMITIVE) {
			PrimitiveTypeInfo ptype = (PrimitiveTypeInfo) type;
			return ptype.getPrimitiveCategory() == PrimitiveCategory.BYTE;
		}
		return false;
	}

	private void debug(String string) {
		System.out.println(string);
	}

	@Override
	public Object deserialize(Writable blob) throws SerDeException {
		byte[] bytes = ((BytesWritable) blob).getBytes();
		debug("Deser: " + Arrays.toString(bytes));
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
			try {
				return (Writable) ptype.getPrimitiveWritableClass().newInstance();
			} catch (Exception e) {
				throw new SerDeException("Unable to create primitive writable: " + type);
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
