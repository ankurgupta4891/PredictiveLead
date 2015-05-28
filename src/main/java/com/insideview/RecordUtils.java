package com.insideview;

import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

public abstract class RecordUtils {

	private final static String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
	private final static String DATE_FORMAT = "yyyy-MM-dd";

	protected String getStringFromMap(Map<byte[], byte[]> map, byte[] field) {
		if (map.containsKey(field))
			return Bytes.toString(map.get(field));
		return null;
	}

	protected Boolean getBooleanFromMap(Map<byte[], byte[]> map, byte[] field) {
		String val = getStringFromMap(map, field);
		if (val == null)
			return null;
		if (val != null && (val.equals("1") || val.equals("0"))) {
			return val.equals("1");
		}
		return Boolean.parseBoolean(getStringFromMap(map, field));
	}

	protected Double getDoubleFromMap(Map<byte[], byte[]> map, byte[] field) {
		String valueString = getStringFromMap(map, field);
		if (valueString != null)
			try {
				return Double.parseDouble(valueString);
			} catch (NumberFormatException ex) {
			}
		return null;
	}

	protected Integer getIntegerFromMap(Map<byte[], byte[]> map, byte[] field) {
		String valueString = getStringFromMap(map, field);
		if (valueString != null)
			try {
				return Integer.parseInt(valueString);
			} catch (NumberFormatException ex) {
			}
		return null;
	}

}
