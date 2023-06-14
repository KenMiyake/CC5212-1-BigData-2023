package org.mdp.spark.cli;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
//import scala.Tuple3;

public class BestReviewsWordsCount {

	
	
	/**
	 * This will be called by spark
	 */
	public static void main(String[] args) {
		
		if(args.length != 2) {
			System.err.println("Usage arguments: inputPath outputPath");
			System.exit(0);
		}
		new BestReviewsWordsCount().run(args[0],args[1]);
	}

	/**
	 * The task body
	 */
	public void run(String inputFilePath, String outputFilePath) {
		/*
		 * Initialises a Spark context with the name of the application
		 *   and the (default) master settings.
		 */
		SparkConf conf = new SparkConf()
				.setAppName(InfoSeriesRating.class.getName());
		JavaSparkContext context = new JavaSparkContext(conf);

		/*
		 * Load the first RDD from the input location (a local file, HDFS file, etc.)
		 */
		JavaRDD<String> inputRDD = context.textFile(inputFilePath);
		
		/*
		 * Filtramos las mejores reseñas con titulo no nulo
		 */
		JavaRDD<String> bestReviews = inputRDD.filter(
				line -> !line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)[1].equals("") && Double.parseDouble(line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)[6])>3
		);
		
		/*
		 * We create a tuple (title,text) where title is the key 
		 */


		JavaRDD<Tuple2<String,String>> reviewsScoreText = bestReviews.map(
				line -> new Tuple2<String,String> (
							line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)[1],
							line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)[9].toLowerCase()
						)
		);
	
		/*
		 * Then we count each word in the text*/
		
		JavaRDD<String> wordRDD = reviewsScoreText.flatMap(tup -> Arrays.<String>asList(tup._2.split(" ")).iterator());
		
		JavaPairRDD<String,Integer> wordOneRDD = wordRDD.mapToPair(word -> new Tuple2<String,Integer>(word, 1));
		JavaPairRDD<String,Integer> wordCount = wordOneRDD.reduceByKey((a, b) -> a + b);
		wordCount.saveAsTextFile(outputFilePath);
		
		context.close();		
		
		

	}	
	
}
