package com.insideview.dao;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.linalg.Vectors;

import com.insideview.database.configuration.DBConfiguration;

public class LRDAO {
	private static final String TABLE = "test-meta";
	private static final byte[] FAMILY = Bytes.toBytes("d");
	private static final byte[] MODEL_ROW = Bytes.toBytes("model");
	private static final byte[] WEIGHTS_COL = Bytes.toBytes("weights");
	private static final byte[] INTERCEPT_COL = Bytes.toBytes("intercept");
	private HConnection connection;
	private static final Log LOG = LogFactory.getLog(LRDAO.class);

	public LRDAO() {
		this.connection = DBConfiguration.getConnection();
	}

	public void storeModel(SVMModel model) throws IOException {
		double[] weights = model.weights().toArray();
		double intercept = model.intercept();
		StringBuilder weightsString = new StringBuilder();
		for (int i = 0; i < weights.length; i++) {
			if (weightsString.length() != 0) {
				weightsString.append(",");
			}
			weightsString.append(weights[i]);
		}
		HTableInterface table = connection.getTable(TABLE);
		Put put = new Put(MODEL_ROW);
		put.add(FAMILY, WEIGHTS_COL, Bytes.toBytes(weightsString.toString()));
		put.add(FAMILY, INTERCEPT_COL, Bytes.toBytes(Double.toString(intercept)));
		try {
			table.put(put);
		} finally {
			if (table != null) {
				table.close();
			}
		}
	}

	public SVMModel getModel() throws IOException {
		HTableInterface table = connection.getTable(TABLE);
		Get get = new Get(MODEL_ROW);
		SVMModel model = null;
		try {
			Result r = table.get(get);
			double intercept = Double.parseDouble(Bytes.toString(r.getValue(FAMILY, INTERCEPT_COL)));
			String weightString = Bytes.toString(r.getValue(FAMILY, WEIGHTS_COL));
			String[] tokens = weightString.split(",");
			double[] weights = new double[tokens.length];
			for (int i = 0; i < tokens.length; i++) {
				weights[i] = Double.parseDouble(tokens[i]);
			}
			model = new SVMModel(Vectors.dense(weights), intercept);
		} finally {
			if (table != null) {
				table.close();
			}
		}
		return model;
	}
}
