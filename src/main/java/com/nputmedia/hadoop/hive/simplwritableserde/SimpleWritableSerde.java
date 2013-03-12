package com.nputmedia.hadoop.hive.simplwritableserde;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
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

		// the column types to return to hive that represent the table,
		// not the underlying data
		List<TypeInfo> tableColumnTypes;
		if (columnTypeProperty.length() == 0) {
			tableColumnTypes = Lists.newArrayList();
		} else {
			// get the column types as defined by the table definition
			rawColumnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
			// convert the column types doing any substitutions needed, this
			// will recursively walk any structs, maps, etc.
			tableColumnTypes = convertTypes(rawColumnTypes);
		}
		assert (columnNames.size() == tableColumnTypes.size());

		// Create row struct type info and object inspector from the column
		// names and converted types
		TypeInfo rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, tableColumnTypes);
		rowObjectInspector = (StructObjectInspector) TypeInfoUtils
				.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
		row = Lists.newArrayListWithCapacity(columnNames.size());
		for (int i = 0; i < rawColumnTypes.size(); i++) {
			row.add(null);
		}

	}

	/**
	 * Converts a typeInfo doing any type substitutions needed, this will
	 * recursively walk any lists, maps, or structs.
	 * 
	 * @param rawType the type to convert
	 * @return the converted type
	 * @throws SerDeException
	 */
	private TypeInfo convertType(TypeInfo rawType) throws SerDeException {
		switch (rawType.getCategory()) {
		case PRIMITIVE: {
			PrimitiveTypeInfo ptype = (PrimitiveTypeInfo) rawType;
			switch (ptype.getPrimitiveCategory()) {
			case BYTE:
				// tinints/bytes are var ints, which are exposed as ints
				return TypeInfoFactory.intTypeInfo;
			case BOOLEAN:
			case SHORT:
			case INT:
			case LONG:
			case FLOAT:
			case DOUBLE:
			case STRING:
				return rawType;
			default:
				throw new SerDeException("Unsupported type by SimpleWritable: " + ptype);
			}
		}
		case LIST: {
			ListTypeInfo listType = (ListTypeInfo) rawType;
			return TypeInfoFactory.getListTypeInfo(convertType(listType.getListElementTypeInfo()));
		}
		case MAP:
			MapTypeInfo mapInfo = (MapTypeInfo) rawType;
			return TypeInfoFactory.getMapTypeInfo(convertType(mapInfo.getMapKeyTypeInfo()),
					convertType(mapInfo.getMapValueTypeInfo()));
		case STRUCT:
			StructTypeInfo structInfo = (StructTypeInfo) rawType;
			return TypeInfoFactory.getStructTypeInfo(structInfo.getAllStructFieldNames(),
					convertTypes(structInfo.getAllStructFieldTypeInfos()));
		default: {
			throw new SerDeException("Unrecognized type: " + rawType.getCategory());
		}
		}
	}

	/**
	 * Convert a list of types use {@link #convertType(TypeInfo)}
	 * 
	 * @param rawTypes
	 * @return
	 * @throws SerDeException
	 */
	private List<TypeInfo> convertTypes(List<TypeInfo> rawTypes) throws SerDeException {
		List<TypeInfo> convertedTypes = Lists.newArrayListWithCapacity(rawTypes.size());
		for (TypeInfo rawType : rawTypes)
			convertedTypes.add(convertType(rawType));
		return convertedTypes;
	}

	@Override
	public Object deserialize(Writable blob) throws SerDeException {
		// convert the bytes into a data input stream which can be read
		byte[] bytes = ((BytesWritable) blob).getBytes();
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));

		// for each column in the row read the field
		for (int i = 0; i < row.size(); i++) {
			try {
				// we use the raw column type, not the exposed type, for reading
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

	/**
	 * Read a field of the given type from the input stream
	 * 
	 * @param type the type of the filed to read
	 * @param in the input tream to read from
	 * @return an object of the read field
	 * @throws SerDeException
	 * @throws IOException
	 */
	private Object readField(TypeInfo type, DataInputStream in) throws SerDeException, IOException {
		// most types are read straight from the input stream using their
		// respective methods
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
			return readStruct((StructTypeInfo) type, in);
		default: {
			throw new SerDeException("Unrecognized type: " + type.getCategory());
		}
		}
	}

	/**
	 * Read a struct from the input stream
	 * 
	 * @param type the type info for the struct
	 * @param in the stream to read from
	 * @return a list containing the deserialized fields of the struct
	 * @throws SerDeException
	 * @throws IOException
	 */
	private List<Object> readStruct(StructTypeInfo type, DataInputStream in) throws SerDeException,
			IOException {
		List<TypeInfo> fieldTypes = type.getAllStructFieldTypeInfos();
		List<Object> struct = Lists.newArrayListWithCapacity(fieldTypes.size());
		for (TypeInfo fieldType : fieldTypes) {
			struct.add(readField(fieldType, in));
		}
		return struct;
	}

	/**
	 * Read a list from the input stream
	 * 
	 * @param type the type info for the list
	 * @param in the stream to read from
	 * @return the deserialized list
	 * @throws SerDeException
	 * @throws IOException
	 */
	private List<Object> readList(ListTypeInfo type, DataInputStream in) throws SerDeException,
			IOException {
		int count = WritableUtils.readVInt(in);
		List<Object> list = Lists.newArrayListWithCapacity(count);
		TypeInfo elementType = type.getListElementTypeInfo();
		for (int i = 0; i < count; i++) {
			list.add(readField(elementType, in));
		}
		return list;
	}

	/**
	 * Read a map from the input stream
	 * 
	 * @param type the type info for the map
	 * @param in the stream to read from
	 * @return the deserialized map
	 * @throws SerDeException
	 * @throws IOException
	 */
	private Map<Object, Object> readMap(MapTypeInfo type, DataInputStream in)
			throws SerDeException, IOException {
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
