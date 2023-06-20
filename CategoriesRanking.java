package org.mdp.spark.cli;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class CategoriesRanking {

	
	
	/**
	 * This will be called by spark
	 */
	public static void main(String[] args) {
		
		if(args.length != 3) {
			System.err.println("Usage arguments: inputPath outputPath");
			System.exit(0);
		}
		new CategoriesRanking().run(args[0],args[1], args[2]);
	}

	/**
	 * The task body
	 */
	public void run(String inputFilePath1, String inputFilePath2, String outputFilePath) {
		/*
		 * Initialize a Spark context with the name of the application
		 *   and the (default) master settings.
		 */
		SparkConf conf = new SparkConf()
				.setAppName(CategoriesRanking.class.getName());
		JavaSparkContext context = new JavaSparkContext(conf);

		/*
		 * Load the first RDD from the input location (a local file, HDFS file, etc.)
		 */
		JavaRDD<String> inputBooksDataRDD = context.textFile(inputFilePath1);
		JavaRDD<String> inputReviewBooksRDD = context.textFile(inputFilePath2);
		
		/*
		 * Here we filter lines that are not TV series or where no episode name is given
		 */
		
		
		String reSplitData= ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
		
		//first we get all the book of which categories are not null 
		JavaRDD<String> ReviewsNoNullCategories = inputBooksDataRDD.filter(
				line -> !line.split(reSplitData, -1)[8].equals("")
		);
		
		
		//Here we create the tuple (title, categories) from books data
		
		JavaPairRDD<String,String> bookCategories= ReviewsNoNullCategories.mapToPair(
				line -> new Tuple2<String,String>(
						line.split(reSplitData,-1)[0],
						line.split(reSplitData,-1)[8])
				);
		//then we get all the reviews with its title field not null
		JavaRDD<String> ReviewsNoNulls = inputReviewBooksRDD.filter(
				line -> !line.split(reSplitData, -1)[1].equals("") 
		);

		
		//we produce with map the tuples (title, score) from the reviews data
		JavaPairRDD<String,Double> reviewsScore = ReviewsNoNulls.mapToPair(
				line -> new Tuple2<String,Double> (
							line.split(reSplitData, -1)[1],
							Double.parseDouble(line.split(reSplitData, -1)[6])
						)
		);	
		
		//now, we count the number of reviews per book and sum the score
		JavaPairRDD<String, Tuple2<Double, Integer>> reviewsToSumCountRating = 
				reviewsScore.aggregateByKey(
						new Tuple2<Double, Integer>(0d, 0), // base value
						(sumCount, rating) -> 
							new Tuple2<Double, Integer>(sumCount._1 + rating, sumCount._2 + 1 ), // combine function
						(sumCountA, sumCountB) -> 
							new Tuple2<Double, Integer>(sumCountA._1 + sumCountB._1, sumCountA._2 + sumCountB._2 )); // reduce function
		
		
		//here we have the tuple (title, (count, avgScore)
		
		JavaPairRDD<String,Tuple2<Integer,Double>> reviewsToCountAndAvgRating = reviewsToSumCountRating.mapToPair(
				tup -> new Tuple2<String,Tuple2<Integer,Double>> (tup._1, new Tuple2<Integer, Double>(tup._2._2,tup._2._1/tup._2._2))
		);
		
		JavaPairRDD<String,Tuple2<Tuple2<Integer, Double>,String>> countCategoriesJoined = reviewsToCountAndAvgRating.join(bookCategories);
		
		//Now, we need to count by categories, in this case we will map to two maps, by count the total of reviews per category and the average rating per category
		
		JavaPairRDD<String,Integer> categoriesCount= countCategoriesJoined.mapToPair(
				tup -> new Tuple2<String, Integer>(tup._2._2,tup._2._1._1));
		
		JavaPairRDD<String,Double> categoriesScore = countCategoriesJoined.mapToPair(
				tup -> new Tuple2<String,Double>(tup._2._2, tup._2._1._2));
		
		
		//Sum all the reviews per category
		JavaPairRDD<String,Integer> categoriesReviewCount = categoriesCount.reduceByKey((a, b) -> a + b);
		
		JavaPairRDD<String, Tuple2<Double, Integer>> categoriesToSumCountRating = 
				categoriesScore.aggregateByKey(
						new Tuple2<Double, Integer>(0d, 0), // base value
						(sumCount, rating) -> 
							new Tuple2<Double, Integer>(sumCount._1 + rating, sumCount._2 + 1 ), // combine function
						(sumCountA, sumCountB) -> 
							new Tuple2<Double, Integer>(sumCountA._1 + sumCountB._1, sumCountA._2 + sumCountB._2 )); // reduce function		
		
		
		
		JavaPairRDD<String,Double> categoriesToAvgRating = categoriesToSumCountRating.mapToPair(
				tup -> new Tuple2<String,Double>(tup._1,tup._2._1/tup._2._2)
		);		
		
		
		//we are almost done, we join the previous results
		
		JavaPairRDD<String,Tuple2<Double,Integer>> countAvgCategoriesJoined = categoriesToAvgRating.join(categoriesReviewCount);
		
		JavaPairRDD<Integer, Tuple2<String,Double>> countKeyForCategories=  countAvgCategoriesJoined.mapToPair(
				tup -> new Tuple2<Integer, Tuple2<String,Double>>(tup._2._2, new Tuple2<String,Double>(tup._1,tup._2._1)));
		
		
		//Finally, we sort the results by number of reviews per category
		
		JavaPairRDD<Integer, Tuple2<String,Double>> countOrderedCategories = countKeyForCategories.sortByKey(false);
		
		
		
		
		

		
		/*
		 * Write the output to local FS or HDFS
		 */
		countOrderedCategories.saveAsTextFile(outputFilePath);
		
		context.close();
	}	
	
}