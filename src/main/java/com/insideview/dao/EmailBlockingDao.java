package com.insideview.dao;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.insideview.BlockingKey;
import com.insideview.DataRecord;
import com.insideview.RecordUtils;
import com.insideview.database.configuration.DBConfiguration;

public class EmailBlockingDao extends RecordUtils {

	private static Logger log = Logger.getLogger(EmailBlockingDao.class);
	private static final String TABLE = "email_blocking_key";
	private static final byte[] FAMILY = Bytes.toBytes("d");
	private static final byte[] COL1 = Bytes.toBytes("exec_id");
	private static final byte[] COL2 = Bytes.toBytes("emp_id");
	private static final byte[] COL3 = Bytes.toBytes("ck_nid");
	private HConnection connection;

	public EmailBlockingDao() {
		this.connection = DBConfiguration.getConnection();
	}

	public void saveBlockingKey(BlockingKey key) throws IOException {
		long start = System.currentTimeMillis();
		HTableInterface table = connection.getTable(TABLE);

		try {
			Put put = new Put(Bytes.toBytes(key.getEmail()));
			put.add(FAMILY, COL1, Bytes.toBytes(String.valueOf(key.getExecId())));
			put.add(FAMILY, COL2, Bytes.toBytes(String.valueOf(key.getEmpId())));
			put.add(FAMILY, COL3, Bytes.toBytes(String.valueOf(key.getCompId())));
			table.put(put);
		} finally {
			if (table != null)
				table.close();
		}
		log.info("took " + (System.currentTimeMillis() - start) + " mills to save blocking key");
	}

	public void loadDataRecordForKey(DataRecord record) throws IOException {
		long start = System.currentTimeMillis();
		HTableInterface table = connection.getTable(TABLE);
		String email = record.getEmail();
		if (StringUtils.isNotBlank(email)) {
			try {
				Get get = new Get(Bytes.toBytes(email));
				Result result = table.get(get);
				Map<byte[], byte[]> map = result.getFamilyMap(FAMILY);
				if (map != null && !map.isEmpty()) {
					Integer execId = getIntegerFromMap(map, COL1);
					Integer empId = getIntegerFromMap(map, COL2);
					Integer ckNid = getIntegerFromMap(map, COL3);
					if (execId != null) {
						record.setExecId(execId);
					}
					if (empId != null) {
						record.setEmpId(empId);
					}
					if (ckNid != null) {
						record.setCompId(ckNid);
					}
				}
			} finally {
				if (table != null)
					table.close();
			}
		}
		System.out.println("took " + (System.currentTimeMillis() - start) + " millis to load record");
	}

}
