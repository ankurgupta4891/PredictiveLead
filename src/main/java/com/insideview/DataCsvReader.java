package com.insideview;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import au.com.bytecode.opencsv.CSVReader;

public class DataCsvReader {

	private static Map<String, DataRecord> dataMap = new HashMap<String, DataRecord>();
	private static DataCsvReader _instance = null;

	private DataCsvReader() {
		try {
			loadData();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static DataCsvReader getInstance() {
		if (_instance == null) {
			synchronized (DataCsvReader.class) {
				if (_instance == null) {
					_instance = new DataCsvReader();
				}
			}
		}
		return _instance;
	}

	private void loadData() throws IOException {
		CSVReader csvReader = new CSVReader(new InputStreamReader(DataCsvReader.class.getClassLoader().getResourceAsStream(
		    "converted.csv"), "UTF-8"));
		csvReader.readNext();
		String[] row;
		try {
			while ((row = csvReader.readNext()) != null) {
				String email = row[5];
				String revenues = row[1];
				DataRecord record = new DataRecord();
				record.setEmail(email);
				record.setLabel(true);
				if (StringUtils.isNotBlank(revenues)) {
					double rev = Double.parseDouble(revenues);
					if (rev > 10) {
						record.setRevenue(rev / 1000000);
					}
				}
				String empCount = row[2];
				if (StringUtils.isNotBlank(empCount)) {
					record.setEmpCount(Integer.parseInt(empCount));
				}
			}
		} finally {
			csvReader.close();
		}
		csvReader = new CSVReader(new InputStreamReader(DataCsvReader.class.getClassLoader().getResourceAsStream(
		    "unconverted.csv"), "UTF-8"));
		csvReader.readNext();
		try {
			while ((row = csvReader.readNext()) != null) {
				String email = row[5];
				String revenues = row[1];
				DataRecord record = new DataRecord();
				record.setEmail(email);
				record.setLabel(false);
				if (StringUtils.isNotBlank(revenues)) {
					double rev = Double.parseDouble(revenues);
					if (rev > 10) {
						record.setRevenue(rev / 1000000);
					}
				}
				String empCount = row[2];
				if (StringUtils.isNotBlank(empCount)) {
					record.setEmpCount(Integer.parseInt(empCount));
				}
			}
		} finally {
			csvReader.close();
		}

	}

	public Map<String, DataRecord> getDataMap() {
		return dataMap;
	}

	public DataRecord getRecord(String email) {
		return dataMap.get(email);
	}
}
