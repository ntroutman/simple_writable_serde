package com.nputmedia.hadoop.hive.simplwritableserde;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
	private ArrayList<Object> row;
	private List<TypeInfo> rawColumnTypes;

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
		if (columnTypeProperty.length() == 0) {
			tableColumnTypes = Lists.newArrayList();
		} else {
			rawColumnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
			tableColumnTypes = Lists.newArrayListWithCapacity(rawColumnTypes.size());
			for (int i = 0; i < rawColumnTypes.size(); i++) {
				TypeInfo rawColumn = rawColumnTypes.get(i);
				// bytes,aka tinyint, are actually treated as var ints
				if (isByte(rawColumn)) {
					tableColumnTypes.add(TypeInfoFactory.intTypeInfo);
				} else {
					tableColumnTypes.add(rawColumn);
				}
			}
		}
		assert (columnNames.size() == tableColumnTypes.size());

		// Create row struct type info and object inspector from the column
		// names and types
		rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, tableColumnTypes);
		rowObjectInspector = (StructObjectInspector) TypeInfoUtils
				.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
		row = Lists.newArrayListWithCapacity(columnNames.size());
		for (int i = 0; i < rawColumnTypes.size(); i++) {
			row.add(null);
		}

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
				row.set(i, readField(rawColumnTypes.get(i), in));
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

	private Object readField(TypeInfo type, DataInputStream in) throws SerDeException, IOException {
		switch (type.getCategory()) {
		case PRIMITIVE: {
			PrimitiveTypeInfo ptype = (PrimitiveTypeInfo) type;
			switch (ptype.getPrimitiveCategory()) {
			case BOOLEAN:
				return in.readBoolean();
			case BYTE:
				// bytes are var ints
				return WritableUtils.readVInt(in);
			case SHORT:
				return in.readShort();
			case INT:
				return in.readInt();
			case LONG:
				return in.readLong();
			case FLOAT:
				return in.readFloat();
			case DOUBLE:
				return in.readDouble();
			case STRING:
				return Text.readString(in);
			default:
				throw new SerDeException("Unsupported type by SimpleWritable: " + ptype);
			}
		}
		case LIST:
			return readList((ListTypeInfo) type, in);
		case MAP:
			return readMap((MapTypeInfo) type, in);
		case STRUCT:
			throw new SerDeException("Struct not supported by SimpleWritable");
		default: {
			throw new RuntimeException("Unrecognized type: " + type.getCategory());
		}
		}
	}

	private Object readList(ListTypeInfo type, DataInputStream in) throws SerDeException,
			IOException {
		int count = WritableUtils.readVInt(in);
		List<Object> list = Lists.newArrayListWithCapacity(count);
		TypeInfo elementType = type.getListElementTypeInfo();
		for (int i = 0; i < count; i++) {
			list.add(readField(elementType, in));
		}
		return list;
	}

	private Object readMap(MapTypeInfo type, DataInputStream in) throws SerDeException, IOException {
		int count = WritableUtils.readVInt(in);
		Map<Object, Object> map = Maps.newHashMapWithExpectedSize(count);
		TypeInfo keyType = type.getMapKeyTypeInfo();
		TypeInfo valueType = type.getMapValueTypeInfo();
		for (int i = 0; i < count; i++) {
			Object key = readField(keyType, in);
			Object value = readField(valueType, in);
			map.put(key, value);
		}
		return map;
	}
}
