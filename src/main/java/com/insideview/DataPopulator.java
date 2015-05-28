package com.insideview;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.SVMModel;

import com.google.gson.Gson;

@SuppressWarnings("serial")
public class DataPopulator {
	private static final Log LOG = LogFactory.getLog(DataPopulator.class);
	private static SVMModel model = null;
	private static final Gson gson = new Gson();

	public static void main(String[] args) throws Exception {
		train(args);
	}

	private static void train(String[] args) throws Exception {
		JavaSparkContext sc = getSparkContext();
		String path1 = "/user/ankurg/data/set2.txt";
		JavaRDD<String> data1 = sc.textFile(path1);

		String path2 = "/user/ankurg/data/set3.txt";
		JavaRDD<String> data2 = sc.textFile(path2);

		JavaRDD<String> finalData = data1.map(new Function<String, String>() {
			public String call(String v1) throws Exception {
				DataRecord rec = DataRecordService.getDataRecordForEmail(v1);
				if (rec == null) {
					LOG.info("null record in 1");
					return null;
				}
				LOG.info("not null record in 1" + rec);
				rec.setLabel(true);
				return gson.toJson(rec);
			}
		}).union(data2.map(new Function<String, String>() {
			public String call(String v1) throws Exception {
				DataRecord rec = DataRecordService.getDataRecordForEmail(v1);
				if (rec == null) {
					LOG.info("null record in 2");
					return null;
				}
				LOG.info("not null record in 2" + rec);
				rec.setLabel(false);
				return gson.toJson(rec);
			}
		})).filter(new Function<String, Boolean>() {

			public Boolean call(String v1) throws Exception {
				return v1 != null;
			}
		});
		finalData.saveAsTextFile("/user/ankurg/data/trData/");

	}

	private static JavaSparkContext getSparkContext() {
		SparkConf conf = new SparkConf();
		conf.setAppName("Data Populator");
		// Following configurations are moved to spark-defaults.conf
		// conf.set("spark.shuffle.spill", "true");
		// conf.set("spark.hadoop.validateOutputSpecs", "false");
		// conf.set("spark.yarn.preserve.staging.files", "true");
		// conf.set("spark.shuffle.memoryFraction", "0.2");
		// conf.set("spark.shuffle.spill.compress", "true");
		// conf.set("spark.rdd.compress", "true");
		// conf.set("spark.storage.memoryFraction", "0.1");
		return new JavaSparkContext(conf);
	}

}
