package com.insideview.dao;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.insideview.DataRecord;
import com.insideview.RecordUtils;
import com.insideview.database.configuration.DBConfiguration;

public class CompanyFieldsDAO extends RecordUtils {
	private static final String TABLE = "company_fields";
	private static final byte[] FAMILY = Bytes.toBytes("d");
	private static final byte[] COL1 = Bytes.toBytes("popularity");
	private static final byte[] COL2 = Bytes.toBytes("fortuneRanking");
	private static final byte[] COL3 = Bytes.toBytes("revenues");
	private static final byte[] COL4 = Bytes.toBytes("employees");
	private HConnection connection;
	private static final Log LOG = LogFactory.getLog(CompanyFieldsDAO.class);

	public CompanyFieldsDAO() {
		this.connection = DBConfiguration.getConnection();
	}

	public void loadData(DataRecord dr) throws IOException {
		if (dr.getCompId() == 0)
			return;
		long start = System.currentTimeMillis();
		HTableInterface table = connection.getTable(TABLE);
		String ckNidString = String.valueOf(dr.getCompId());
		try {
			Get get = new Get(Bytes.toBytes(ckNidString));
			Result result = table.get(get);
			fillData(dr, result.getFamilyMap(FAMILY));
		} finally {
			if (table != null)
				table.close();
		}
		System.out.println("took " + (System.currentTimeMillis() - start) + " millis to fill data record");
	}

	private void fillData(DataRecord dr, NavigableMap<byte[], byte[]> familyMap) {
		if (familyMap == null || familyMap.isEmpty()) {
			return;
		}
		Integer popularity = getIntegerFromMap(familyMap, COL1);
		if (popularity != null) {
			dr.setPopularity(popularity);
		}
		Integer fortuneListed = getIntegerFromMap(familyMap, COL2);
		if (fortuneListed != null) {
			dr.setFortuneListed(fortuneListed == -1 ? false : true);
		}
		Double revenue = getDoubleFromMap(familyMap, COL3);
		if (revenue != null && dr.getRevenue() == 0.0) {
			dr.setRevenue(revenue);
		}
		Integer empCount = getIntegerFromMap(familyMap, COL4);
		if (empCount != null && dr.getEmpCount() == 0) {
			dr.setEmpCount(empCount);
		}
	}

}
