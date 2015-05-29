package com.insideview;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.insideview.dao.CompanyFieldsDAO;
import com.insideview.dao.ContactDAO;
import com.insideview.dao.EmailBlockingDao;

public class DataRecordService {

	private static EmailBlockingDao emailBlockingDao = new EmailBlockingDao();
	private static CompanyFieldsDAO companyFieldsDAO = new CompanyFieldsDAO();
	private static ContactDAO contactDAO = new ContactDAO();
	private static final Log LOG = LogFactory.getLog(DataRecordService.class);
	private static DataCsvReader dataReader = DataCsvReader.getInstance();

	public static DataRecord getDataRecordForEmail(String email) {
		DataRecord record = dataReader.getRecord(email);
		if (record == null) {
			record = new DataRecord();
			record.setEmail(email);
		}
		try {
			record = build(record);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return record;
	}

	private static DataRecord build(DataRecord record) throws IOException {
		emailBlockingDao.loadDataRecordForKey(record);
		if (record.getExecId() == 0) {
			return null;
		}
		LOG.info("Matched record : " + record);
		contactDAO.loadData(record);
		companyFieldsDAO.loadData(record);
		LOG.info("Matched record 2 : " + record);
		if (!record.isValid()) {
			return null;
		}
		LOG.info("Matched record 3: " + record);
		return record;
	}

	public static void main(String[] args) throws IOException {
		DataRecord record = new DataRecord();
		record.setEmpCount(1001000);
		record.setJobLevel(Integer.valueOf(4).shortValue());
		record.setRevenue(500);
		record.setJobFunction(6);
		record.setPopularity(2500);
		System.out.println(LogisticRegression.predict(record));
	}
}
