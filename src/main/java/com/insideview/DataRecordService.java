package com.insideview;

import java.io.IOException;

import com.insideview.dao.CompanyDAO;
import com.insideview.dao.CompanyFieldsDAO;
import com.insideview.dao.EmailBlockingDao;

public class DataRecordService {

	private static EmailBlockingDao emailBlockingDao = new EmailBlockingDao();
	private static CompanyFieldsDAO companyFieldsDAO = new CompanyFieldsDAO();
	private static CompanyDAO companyDAO = new CompanyDAO();

	public DataRecord getDataRecordForEmail(String email) {
		DataRecord record = new DataRecord();
		record.setEmail(email);
		try {
			build(record);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return record;
	}

	private void build(DataRecord record) throws IOException {
		emailBlockingDao.loadDataRecordForKey(record);
		companyDAO.loadData(record);
		companyFieldsDAO.loadData(record);
	}
}
