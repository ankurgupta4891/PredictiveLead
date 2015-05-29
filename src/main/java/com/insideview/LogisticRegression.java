package com.insideview;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;

import com.google.gson.Gson;
import com.insideview.dao.LRDAO;

@SuppressWarnings("serial")
public class LogisticRegression {
	private static final Log LOG = LogFactory.getLog(LogisticRegression.class);
	private static LogisticRegressionModel model = null;
	private static LRDAO lrdao = new LRDAO();
	private static Gson gson = new Gson();

	public static void main(String[] args) throws Exception {
		train(args);
	}

	private static void train(String[] args) throws Exception {
		JavaSparkContext sc = getSparkContext();
		String path = "/user/iv/trainingSet";
		JavaRDD<String> data = sc.textFile(path);
		JavaRDD<LabeledPoint> parsedData = data.map(new Function<String, LabeledPoint>() {
			public LabeledPoint call(String line) {
				DataRecord r = gson.fromJson(line, DataRecord.class);
				int res = 0;
				Vector v = getVector(r);
				double[] x = v.toArray();
				if (x[0] >= 5 || x[4] >= 100 || x[3] >= 1000) {
					res = 1;
				}
				System.out.println("input is: " + getVector(r) + " y " + res);
				return new LabeledPoint(Double.valueOf(res), v);
			}
		});

		// Split initial RDD into two... [60% training data, 40% testing data].
		JavaRDD<LabeledPoint> training = parsedData.sample(false, 0.6, 11L);
		training.cache();
		JavaRDD<LabeledPoint> test = parsedData.subtract(training);

		// Run training algorithm to build the model.
		int numIterations = 10000;
		LogisticRegressionWithLBFGS s = new LogisticRegressionWithLBFGS();
		s.addIntercept();
		final LogisticRegressionModel model2 = s.run(parsedData.rdd());
		final LogisticRegressionModel model = LogisticRegressionWithSGD.train(training.rdd(), numIterations);
		// Clear the default threshold.
		model.clearThreshold();
		// Compute raw scores on the test set.
		JavaRDD<Tuple2<Object, Object>> scoreAndLabels = test.map(new Function<LabeledPoint, Tuple2<Object, Object>>() {
			public Tuple2<Object, Object> call(LabeledPoint p) {
				Double score = model.predict(p.features());
				Double score2 = model2.predict(p.features());
				System.out.println(" model 1 predicted " + score + " for " + p);
				System.out.println(" model 2 predicted " + score2 + " for " + p);
				return new Tuple2<Object, Object>(score, p.label());
			}
		});

		// Get evaluation metrics.
		BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
		double auROC = metrics.areaUnderROC();
		lrdao.storeModel(model2);
		System.out.println("model is " + model);
		System.out.println("Area under ROC = " + auROC);

	}

	private static JavaSparkContext getSparkContext() {
		SparkConf conf = new SparkConf();
		conf.setAppName("Company ET");
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

	public static Vector getVector(DataRecord record) {
		double x1 = (record.getJobLevel());
		double x2 = (record.getJobFunction());
		double x3 = (record.getPopularity());
		double x4 = (record.getEmpCount());
		double x5 = (record.getRevenue());
		double x6 = (x3 * 10 + x4 * 7 + x5);
		double[] x = new double[] { x1, x2, x3, x4, x5 };
		Vector v = Vectors.dense(x);
		return v;
	}

	public static double predict(DataRecord record) throws IOException {
		if (model == null) {
			synchronized (LogisticRegression.class) {
				if (model == null) {
					model = lrdao.getModel();
				}
			}
		}
		if (record == null) {
			throw new NullPointerException("input record can not be null");
		}
		Vector v = getVector(record);
		return model.predict(v);
	}
}
