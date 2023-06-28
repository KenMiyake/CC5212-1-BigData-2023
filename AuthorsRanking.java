package org.mdp.spark.cli;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class AuthorsRanking {

	
	
	/**
	 * This will be called by spark
	 */
	public static void main(String[] args) {
		
		if(args.length != 3) {
			System.err.println("Usage arguments: inputPath outputPath");
			System.exit(0);
		}
		new AuthorsRanking().run(args[0],args[1], args[2]);
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
				.setAppName(AuthorsRanking.class.getName());
		JavaSparkContext context = new JavaSparkContext(conf);

		/*
		 * Load the first RDD from the input location (a local file, HDFS file, etc.)
		 */
		JavaRDD<String> inputBooksDataRDD = context.textFile(inputFilePath1);
		JavaRDD<String> inputReviewBooksRDD = context.textFile(inputFilePath2);
		

		
		String reSplitData= ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
		
		//first we get all the book of which authors are not null 
		JavaRDD<String> ReviewsNoNullAuthors = inputBooksDataRDD.filter(
				line -> line.split(reSplitData, -1).length != 1  && !line.split(reSplitData, -1)[2].equals("") 
		);
		
		
		//Here we create the tuple (title, authors) from books data
		//With the following R.E. we clean the authors column
		String reCleanAuthors= "\\[|'|\\]"; //originally we did use this
		//but think that is better to clean just the [] symbols
		//then slit by reSplitData

		//if there is time i'll try that


		JavaPairRDD<String,String> bookAuthors= ReviewsNoNullAuthors.mapToPair(
				line -> new Tuple2<String,String>(
						line.split(reSplitData,-1)[0],
						line.split(reSplitData,-1)[2].replaceAll(reCleanAuthors, ""))
				);
		
		//Now we want to create a row for each author, this is usefull for books that have multiple authors
		JavaRDD<String> mappedAuthors = bookAuthors.flatMap(
				tup -> {
					String [] autores= tup._2.split(",");
					for(int i=0;i<autores.length;i++) {
						autores[i]=tup._1+"#"+autores[i];}
					return Arrays.<String>asList(autores).iterator();
					}
				);
		
		
		JavaRDD<String> mappedAuthorsFiltered= mappedAuthors.filter(line-> line.split("#").length !=1);
		//Here we have the pair (title, author)
		JavaPairRDD<String,String> titleAuthorRDD = mappedAuthorsFiltered.mapToPair(line -> new Tuple2<String,String>(line.split("#")[0], line.split("#")[1]));
		
		//then we get all the reviews with its title field not null
		JavaRDD<String> ReviewsNoNulls = inputReviewBooksRDD.filter(
				line -> line.split(reSplitData, -1).length != 1 && !line.split(reSplitData, -1)[1].equals("") && !line.split(reSplitData, -1)[6].equals("review/score")
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
		
		//Here we join by title, so we have the tuple (title, ((count, avgScore), author))
		
		JavaPairRDD<String,Tuple2<Tuple2<Integer, Double>,String>> countAuthorsJoined = reviewsToCountAndAvgRating.join(titleAuthorRDD);
		
		//Now, we need to count by author, in this case we will map to two maps, by count the total of reviews per author and the average rating per author
		
		JavaPairRDD<String,Integer> AuthorsCount= countAuthorsJoined.mapToPair(
				tup -> new Tuple2<String, Integer>(tup._2._2,tup._2._1._1));
		
		JavaPairRDD<String,Double> AuthorsScore = countAuthorsJoined.mapToPair(
				tup -> new Tuple2<String,Double>(tup._2._2, tup._2._1._2));
		
		
		//Sum all the reviews per author
		JavaPairRDD<String,Integer> authorsReviewCount = AuthorsCount.reduceByKey((a, b) -> a + b);
		
		JavaPairRDD<String, Tuple2<Double, Integer>> authorsToSumCountRating = 
				AuthorsScore.aggregateByKey(
						new Tuple2<Double, Integer>(0d, 0), // base value
						(sumCount, rating) -> 
							new Tuple2<Double, Integer>(sumCount._1 + rating, sumCount._2 + 1 ), // combine function
						(sumCountA, sumCountB) -> 
							new Tuple2<Double, Integer>(sumCountA._1 + sumCountB._1, sumCountA._2 + sumCountB._2 )); // reduce function		
		
		
		
		JavaPairRDD<String,Double> authorsToAvgRating = authorsToSumCountRating.mapToPair(
				tup -> new Tuple2<String,Double>(tup._1,tup._2._1/tup._2._2)
		);		
		
		
		//we are almost done, we join the previous results
		
		JavaPairRDD<String,Tuple2<Double,Integer>> countAvgAuthorsJoined = authorsToAvgRating.join(authorsReviewCount);
		
		JavaPairRDD<Integer, Tuple2<String,Double>> countKeyForAuthors=  countAvgAuthorsJoined.mapToPair(
				tup -> new Tuple2<Integer, Tuple2<String,Double>>(tup._2._2, new Tuple2<String,Double>(tup._1,tup._2._1)));
		
		
		//Finally, we sort the results by number of reviews per author
		
		JavaPairRDD<Integer, Tuple2<String,Double>> countOrderedAuthors = countKeyForAuthors.sortByKey(false);
		
		
		
		
		

		
		/*
		 * Write the output to local FS or HDFS
		 */
		countOrderedAuthors.saveAsTextFile(outputFilePath);
		
		context.close();
	}	
	
}