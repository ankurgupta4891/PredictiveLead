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

public class ContactDAO extends RecordUtils {
	private static final String TABLE = "contact_fields";
	private static final byte[] FAMILY = Bytes.toBytes("d");
	private static final byte[] COL1 = Bytes.toBytes("jobLevel");
	private static final byte[] COL2 = Bytes.toBytes("jobBucket");
	private HConnection connection;
	private static final Log LOG = LogFactory.getLog(ContactDAO.class);

	public ContactDAO() {
		this.connection = DBConfiguration.getConnection();
	}

	public void loadData(DataRecord dr) throws IOException {
		if (dr.getExecId() == 0 || dr.getEmpId() == 0)
			return;
		long start = System.currentTimeMillis();
		HTableInterface table = connection.getTable(TABLE);
		String idString = String.valueOf(dr.getExecId()) + "_" + String.valueOf(dr.getEmpId());
		try {
			Get get = new Get(Bytes.toBytes(idString));
			Result result = table.get(get);
			fillData(dr, result.getFamilyMap(FAMILY));
		} finally {
			if (table != null)
				table.close();
		}
		System.out.println("took " + (System.currentTimeMillis() - start) + " millis to fill data record");
	}

	private void fillData(DataRecord dr, NavigableMap<byte[], byte[]> familyMap) {

		Integer jobLevel = getIntegerFromMap(familyMap, COL1);
		if (jobLevel != null) {
			dr.setJobLevel(jobLevel.shortValue());
		}
		Integer jobBucket = getIntegerFromMap(familyMap, COL2);
		if (jobBucket != null) {
			dr.setJobFunction(jobBucket);
		}

	}

}
