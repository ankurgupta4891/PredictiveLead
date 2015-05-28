package com.insideview.database.configuration;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class DBConfiguration {
	private static HConnection connection;

	public static HConnection getConnection() {
		if (connection == null) {
			synchronized (DBConfiguration.class) {
				if (connection == null) {
					try {
						connection = HConnectionManager.createConnection(getConf());
						Runtime.getRuntime().addShutdownHook(new Thread() {

							@Override
							public void run() {
								try {
									Thread.sleep(5000);
									if (connection != null)
										connection.close();
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						});
					} catch (IOException e) {
						// System.out.println("Error while creating Hbase connection");
						// e.printStackTrace(System.out);
						throw new RuntimeException("Error while creating Hbase connection", e);
					}
				}
			}
		}
		return connection;
	}

	public static Configuration getConf() {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.24.2.77");
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
		return conf;
	}

	public static void main(String[] args) throws Exception {
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("d"));
		scan.setCacheBlocks(false);
		scan.setCaching(10);
		scan.setBatch(10);
		scan.setSmall(false);
		HConnection con = getConnection();
		HTableInterface table = con.getTable(Bytes.toBytes("unmatched_vendor_data"));
		ResultScanner scanner = table.getScanner(scan);
		for (Result r = scanner.next(); r != null; r = scanner.next()) {
			System.out.println("scanning loop ..");
			System.out.println(Bytes.toString(r.getValue(Bytes.toBytes("d"), Bytes.toBytes("vendor_data"))));
		}
	}
}
