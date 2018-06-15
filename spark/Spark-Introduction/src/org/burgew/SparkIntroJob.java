package org.burgew;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class SparkIntroJob {

	static private String FIND_LINE = "MOBY DICK; OR THE WHALE";

	static boolean found = false;
	
	protected static boolean foundYet(String line) {
		if ((!found) & line.contains(FIND_LINE))
			found = true;
			
		return found;
	}
	
	protected static String reverseLine(String line) {
        byte [] strAsByteArray = line.getBytes();
        
        byte [] result = 
                   new byte [strAsByteArray.length];
 
        // Store result in reverse order into the
        // result byte[]
        for (int i = 0; i<strAsByteArray.length; i++)
            result[i] = 
             strAsByteArray[strAsByteArray.length-i-1];
        
        return new String(result);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) {
		//this controls a lot of spark related logging
		//comment or change logging level as needed
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		if (args.length != 2) {
			System.err.println("Error, must be called with <input path> <output folder>");
			System.exit(-1);
		}

		long timestamp = System.currentTimeMillis();
		long shorter_timestamp = timestamp/1000;
		String textFilePath = args[0];
		String outputFolder = args[1]+"/run."+shorter_timestamp;
				
		SparkConf sparkConf = new SparkConf().setAppName("HW6");
		if (!sparkConf.contains("spark.master")) {
			//this sets the job to use 2 executors locally
			sparkConf.setMaster("local[2]");
		}

		try(
				//these will auto close
				SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
				JavaSparkContext sc =   new JavaSparkContext(spark.sparkContext()); 
		){

			JavaRDD<String> text_lines = sc.textFile(textFilePath);	
			text_lines.persist(StorageLevel.MEMORY_AND_DISK());
			
			found = false;
			
			JavaRDD<String> goodLines = text_lines.filter(line -> foundYet(line));
			long goodLinesCount = goodLines.count();
			
			System.out.println("\nQuestion 6: Found " + goodLinesCount + " lines starting with the one including text \"" + FIND_LINE + "\"");
			goodLines.saveAsTextFile(outputFolder);
			
			JavaRDD<String> goodWords = goodLines.flatMap(
				      new FlatMapFunction<String, String>() {
						private static final long serialVersionUID = 1L;

						public Iterator<String> call(String x) {
				            return (Iterator<String>) Arrays.asList(x.split(" ")).iterator();
				          }});					
			JavaRDD<String> alphaWords = goodWords.map(tuple -> (tuple.toLowerCase().replaceAll("[^a-zA-Z]", "")));
			JavaPairRDD<String, Integer> wordCounts =  alphaWords.mapToPair(word -> new Tuple2<>(word,1)).reduceByKey((x,y) -> x+y).sortByKey();
			
			System.out.println("\nQuestion 7: The number of unique alpha words is " + wordCounts.count());
			for (Tuple2<String, Integer> tuple : wordCounts.take(10)) {
				System.out.println("Word: \"" + tuple._1 + "\", Count: " + tuple._2);
			}
			
		    JavaPairRDD<Character, Integer> charCounts = wordCounts.flatMap(
		    	      new FlatMapFunction<Tuple2<String, Integer>, Character>() {
		    	        private static final long serialVersionUID = 1L;

						@Override
		    	        public Iterator<Character> call(Tuple2<String, Integer> s) {
		    	          Collection<Character> chars = new ArrayList<Character>(s._1().length());
		    	          for (char c : s._1().toCharArray()) {
		    	            chars.add(c);
		    	          }
		    	          return chars.iterator();
		    	        }
		    	      }
		    	    ).mapToPair(
		    	      new PairFunction<Character, Character, Integer>() {
		    	        private static final long serialVersionUID = 1L;

						@Override
		    	        public Tuple2<Character, Integer> call(Character c) {
		    	          return new Tuple2<Character, Integer>(c, 1);
		    	        }
		    	      }
		    	    ).reduceByKey(
		    	      new Function2<Integer, Integer, Integer>() {
		    	        private static final long serialVersionUID = 1L;

						@Override
		    	        public Integer call(Integer i1, Integer i2) {
		    	          return i1 + i2;
		    	        }
		    	      }
		    	    ).sortByKey();
		    
			System.out.println("\nQuestion 8: The number of unique alpha characters is " + charCounts.count());
			for (Tuple2<Character, Integer> tuple : charCounts.take(26)) {
				System.out.println("Character: '" + tuple._1 + "', Count: " + tuple._2);
			}

			JavaRDD<Object> countsChars = charCounts.map(tuple -> new Tuple2<Integer, Character>(tuple._2, tuple._1));
					
			JavaRDD<Object> freqCounts = countsChars.sortBy(
					new Function() {
						private static final long serialVersionUID = 1L;

				public Integer call( Object tuple_obj ) {
					Tuple2<Integer, Character> tuple = (Tuple2<Integer, Character>)tuple_obj;
					return tuple._1();
				}
			}, found, 0);
			
			System.out.println("\nQuestion 9: Those characters in descending order of frequency:");
			for (Object tuple_obj : freqCounts.take(26)) {
				Tuple2<Integer, Character> tuple = (Tuple2<Integer, Character>)tuple_obj;
				System.out.println("Count: '" + tuple._1 + "', Character: " + tuple._2);
			}
			
			JavaRDD<String> reversed_lines = goodLines.map(new Function<String, String>(){
				private static final long serialVersionUID = 1L;

				@Override
				public String call(String line) throws Exception {
					// TODO Auto-generated method stub
					return reverseLine(line);
				}
				
			});
			
			System.out.println("\nQuestion 10: Here are the first 20 lines, with reversed text:");
			for (String line : reversed_lines.take(20)) {
				System.out.println(line);
			}				

		}
	}
}