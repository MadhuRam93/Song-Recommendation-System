package org.myorg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Vector;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.myorg.Preprocess.Map_Preprocess;
import org.myorg.Preprocess.Reduce_Preprocess;

/*
 * Program to form song pairs, compute similarity of each pair and form list of similar songs
 * Change Map & Reduce classes on line 95 & 96 for:
 * 
 * Cosine Similarity: job2.setMapperClass( Map_CosineSimilarity .class);
 * job2.setReducerClass( Reduce_CosineSimilarity .class);
 * 
 * Pearson Similarity: job2.setMapperClass( Map_PearsonSimilarity .class);
 * job2.setReducerClass( Reduce_PearsonSimilarity .class);
 * 	
 * Jaccard Similarity: job2.setMapperClass( Map_JaccardSimilarity .class);
 * job2.setReducerClass( Reduce_JaccardSimilarity .class);
 * 
 * Euclidean Similarity: job2.setMapperClass( Map_EuclidSimilarity .class);
 * job2.setReducerClass( Reduce_EuclidSimilarity .class);
 * 
 */

public class Calc_Similarity extends Configured implements Tool {

	public static void main( String[] args) throws  Exception {

		long start = new Date().getTime();

		int res  = ToolRunner .run( new Calc_Similarity(), args);

		long end = new Date().getTime();
		System.out.println(">>>> RUNNING TIME of COSINE SIMILARITY <<<<<<	" + (end-start) + "milliseconds");

		System .exit(res);
	}

	public int run( String[] args) throws  Exception {

		Configuration conf1 = new Configuration();

		String intermediate_dir = "intermediate_dir";
		Path output_path = new Path(args[1]);
		Path intermediate_path = new Path(intermediate_dir);
		FileSystem hdfs = FileSystem.get(conf1);

		try {
			if(hdfs.exists(output_path)){
				hdfs.delete(output_path, true);
			} 
			if(hdfs.exists(intermediate_path)){
				hdfs.delete(intermediate_path, true);
			} 
		} catch (IOException e) {
			e.printStackTrace();
		}

		Job job1  = Job .getInstance(conf1, "Preprocess");
		job1.setJarByClass(Preprocess.class);

		Path intermed1 = new Path(intermediate_path, "intermed1");

		FileInputFormat.addInputPaths(job1,  args[0]);
		FileOutputFormat.setOutputPath(job1, intermed1);

		job1.setMapperClass( Map_Preprocess .class);
		job1.setReducerClass( Reduce_Preprocess .class);

		job1.setOutputKeyClass( Text .class);
		job1.setOutputValueClass( Text .class);

		int success1 =  job1.waitForCompletion( true)  ? 0 : 1;

		int success2 = 1;
		int success3 = 1;
		if(success1 == 0){

			Configuration conf2 = new Configuration();

			Job job2  = Job .getInstance(conf2, "Calculate_Cosine_Similarity");
			job2.setJarByClass(Calc_Similarity.class);

			Path intermed2 = new Path(intermediate_path, "intermed2");

			FileInputFormat.addInputPath(job2,  intermed1);
			FileOutputFormat.setOutputPath(job2, intermed2);

			job2.setMapperClass( Map_CosineSimilarity .class);
			job2.setReducerClass( Reduce_CosineSimilarity .class);

			job2.setOutputKeyClass( Text .class);
			job2.setOutputValueClass( Text .class);

			success2 =  job2.waitForCompletion( true)  ? 0 : 1;


			if(success2 == 0){
				Configuration conf3 = new Configuration();

				Job job3  = Job .getInstance(conf3, "Cosine_Based_Recommendation");
				job3.setJarByClass(Preprocess.class);

				FileInputFormat.addInputPath(job3,  intermed2);
				FileOutputFormat.setOutputPath(job3, output_path);

				job3.setMapperClass( Map_C_Recommendation .class);
				job3.setReducerClass( Reduce_C_Recommendation .class);

				job3.setOutputKeyClass( Text .class);
				job3.setOutputValueClass( Text .class);

				success3 =  job3.waitForCompletion( true)  ? 0 : 1;
			}
		}      
		return success3;
	}

	/*
	 * Mapper function 2
	 * input: offset & content (lineText) of every line in the input file: <userID songID=rating_List>
	 * output: <key, value> pairs -> <song1$$song2	rating1@@rating2>
	 * Mapper function reads the input file, one line at a time, converts it to String and splits the line at space & delimiter to extract all the song-rating pairs.
	 * Each song & rating is separated & stored in an ArrayList.
	 * Pairs of songs are formed & corresponding pairs of rating are sent to reducer.
	 */

	public static class Map_CosineSimilarity extends Mapper<LongWritable ,  Text ,  Text ,  Text > {

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			ArrayList<Integer> songs = new ArrayList<Integer>();
			ArrayList<Double> rating = new ArrayList<Double>();

			String line  = lineText.toString();

			String values_list = StringUtils.substringAfter(line, "	");         
			String[] song_rating_list = StringUtils.split(values_list, ",");

			if(song_rating_list.length >1){
				for(String song_rating : song_rating_list){

					String[] parts = StringUtils.split(song_rating, "=");
					songs.add(Integer.parseInt(parts[0]));
					rating.add(Double.parseDouble(parts[1]));
				}

				int len = songs.size();

				for(int i=0; i<len-1; i++){
					for(int j=i+1; j<len; j++){

						String song_pair = songs.get(i) + "$$" + songs.get(j);
						String rating_pair = rating.get(i) + "@@" + rating.get(j);

						context.write(new Text(song_pair), new Text(rating_pair));
						System.out.println(song_pair + "  >>>  " +  rating_pair);        		 
					}
				}
			}	         	 
		}
	}

	/*
	 * Reducer function 2
	 * input: output <key, value> pairs from Mapper2 -> <song1$$song2	[list of rating1@@rating2] >
	 * output: <key, value> pairs -> <song1$$song2 similarity_value>
	 * For each pair of songs, reducer calculates cosine similarity. If this score is greater than 0.5,reducer writes song-pair & similarity score to output. 
	 */

	public static class Reduce_CosineSimilarity extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
		@Override 
		public void reduce( Text song_pair,  Iterable<Text > rating_pair_list,  Context context)
				throws IOException,  InterruptedException {

			Vector<Double> rating1 = new Vector<Double>();
			Vector<Double> rating2 = new Vector<Double>();

			double dot_product = 0.0;
			double norm1 = 0.0;
			double norm2 = 0.0;
			double cos_theta = 0.0;

			for(Text val : rating_pair_list){

				String ratings = val.toString();

				String[] rating = StringUtils.split(ratings, "@@");

				rating1.addElement(Double.parseDouble(rating[0]));
				rating2.addElement(Double.parseDouble(rating[1]));
			}

			/*System.out.println(">>>> COSINE VECTORS <<<<<<	" + song_pair.toString());

				Enumeration<Double> e1 = rating1.elements();
				Enumeration<Double> e2 = rating2.elements();

				while(e1.hasMoreElements()){
					System.out.print(e1.nextElement() + "	");
				}
				System.out.println();
				while(e2.hasMoreElements()){
					System.out.print(e2.nextElement() + "	");
				}*/

			if(rating1.size() == rating2.size() && rating1.size() > 1){
				for(int i=0; i<rating1.size(); i++){

					dot_product  = dot_product + rating1.get(i) * rating2.get(i);

					norm1 = norm1 + Math.pow(rating1.get(i), 2);
					norm2 = norm2 + Math.pow(rating2.get(i), 2); 
				}

				cos_theta = dot_product / (Math.sqrt(norm1) * Math.sqrt(norm2));

				if(cos_theta > 0.5){
					context.write(song_pair, new DoubleWritable(cos_theta));
					System.out.println(song_pair + "	>>>	" + cos_theta);
				}
			}
		}
	}

	/*
	 * Mapper function 3
	 * input: offset & content (lineText) of every line in the input file: <userID songID=rating_List>
	 * output: <key, value> pairs -> <song1$$song2	rating1@@rating2>
	 * Mapper function reads the input file, one line at a time, converts it to String and splits the line at space & delimiter to extract all the song-rating pairs.
     * Each song & rating is separated & stored in an ArrayList.
     * Pairs of songs are formed & corresponding pairs of rating are sent to reducer.
	 */

	public static class Map_PearsonSimilarity extends Mapper<LongWritable ,  Text ,  Text ,  Text > {

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			ArrayList<Integer> songs = new ArrayList<Integer>();
			ArrayList<Double> rating = new ArrayList<Double>();

			String line  = lineText.toString();

			String values_list = StringUtils.substringAfter(line, "	");         
			String[] song_rating_list = StringUtils.split(values_list, ",");

			if(song_rating_list.length >1){
				for(String song_rating : song_rating_list){

					String[] parts = StringUtils.split(song_rating, "=");
					songs.add(Integer.parseInt(parts[0]));
					rating.add(Double.parseDouble(parts[1]));
				}

				int len = songs.size();

				for(int i=0; i<len-1; i++){
					for(int j=i+1; j<len; j++){

						String song_pair = songs.get(i) + "$$" + songs.get(j);
						String rating_pair = rating.get(i) + "@@" + rating.get(j);

						context.write(new Text(song_pair), new Text(rating_pair));
						System.out.println(song_pair + "  >>>  " +  rating_pair);        		 
					}
				}
			} 
		}
	}

	/*
	 * Reducer function 3
	 * input: output <key, value> pairs from Mapper3 -> <song1$$song2	[list of rating1@@rating2] >
	 * output: <key, value> pairs -> <song1$$song2 similarity_value>
	 * For each pair of songs, reducer calculates pearson similarity. If this score is greater than 0,reducer writes song-pair & similarity score to output. 
	 */

	public static class Reduce_PearsonSimilarity extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
		@Override 
		public void reduce( Text song_pair,  Iterable<Text > rating_pair_list,  Context context)
				throws IOException,  InterruptedException {

			Vector<Double> rating1 = new Vector<Double>();
			Vector<Double> rating2 = new Vector<Double>();

			double sum1 = 0.0;
			double sum2 = 0.0;

			for(Text val : rating_pair_list){

				String ratings = val.toString();

				String[] rating = StringUtils.split(ratings, "@@");

				rating1.addElement(Double.parseDouble(rating[0]));
				rating2.addElement(Double.parseDouble(rating[1]));
			}

			/*System.out.println(">>>> PEARSON VECTORS <<<<<<	" + song_pair.toString());

		    	  Enumeration<Double> e1 = rating1.elements();
		    	  Enumeration<Double> e2 = rating2.elements();

		    	  System.out.println("Rating1 List--------------------------------------------");
		    	  while(e1.hasMoreElements()){
		    		  System.out.print(e1.nextElement() + "	");
		    	  }
		    	  System.out.println();
		    	  System.out.println("Rating2 List--------------------------------------------");
		    	  while(e2.hasMoreElements()){
		    		  System.out.print(e2.nextElement() + "	");
		    	  }*/

			if(rating1.size() == rating2.size() && rating1.size() > 1){

				int len = rating1.size();
				/*System.out.println(len);*/

				for(int i=0; i<len; i++){

					sum1 = sum1 + rating1.get(i);
					sum2 = sum2 + rating2.get(i);    			  
				}

				double average1 = sum1 / len;	    	  /*System.out.println(sum1 + "	" + average1);*/
				double average2 = sum2 / len;				 /*System.out.println(sum2 + "	" + average2);*/
				double denom1 = 0.0;
				double denom2 = 0.0;

				for(int i=0; i<len; i++){

					denom1 = denom1 + Math.pow(rating1.get(i)-average1, 2);
					denom2 = denom2 + Math.pow(rating2.get(i)-average2, 2);
				}

				double denom = Math.sqrt(denom1 * denom2);		 /*System.out.println(denom1 + "	" + denom2 + "		" + denom);*/
				double pearson_coeff = 0.0;

				for(int i=0; i<len; i++){

					double num = (rating1.get(i) - average1) * (rating2.get(i) - average2); 
					pearson_coeff = pearson_coeff + (num/denom);
				}

				if(Double.isNaN(pearson_coeff)){
					pearson_coeff = 0.0;
				} 
				if(pearson_coeff > 0){
					context.write(song_pair, new DoubleWritable(pearson_coeff));
					/*System.out.println(song_pair + "	>>>	" + pearson_coeff);*/
				}
			}
		}
	}

	/*
	 * Mapper function 4
	 * input: offset & content (lineText) of every line in the input file: <userID songID=rating_List>
	 * output: <key, value> pairs -> <song1$$song2	rating1@@rating2>
	 * Mapper function reads the input file, one line at a time, converts it to String and splits the line at space & delimiter to extract all the song-rating pairs.
     * Each song & rating is separated & stored in an ArrayList.
     * Pairs of songs are formed & corresponding pairs of rating are sent to reducer.
	 */
	   public static class Map_JaccardSimilarity extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
		   
	      public void map( LongWritable offset,  Text lineText,  Context context)
	        throws  IOException,  InterruptedException {
	    	    
	    	  ArrayList<Integer> songs = new ArrayList<Integer>();
	    	  ArrayList<Double> rating = new ArrayList<Double>();
	    	  
	         String line  = lineText.toString();

	         String values_list = StringUtils.substringAfter(line, "	");         
	         String[] song_rating_list = StringUtils.split(values_list, ",");
	         
	         if(song_rating_list.length >1){
	        	 for(String song_rating : song_rating_list){
	            	 
	            	 String[] parts = StringUtils.split(song_rating, "=");
	            	 songs.add(Integer.parseInt(parts[0]));
	            	 rating.add(Double.parseDouble(parts[1]));
	             }
	        	 
	        	 int len = songs.size();
	             
	        	 for(int i=0; i<len-1; i++){
	            	 for(int j=i+1; j<len; j++){
	    	        	 
	    	        	 String song_pair = songs.get(i) + "$$" + songs.get(j);
	    	        	 String rating_pair = rating.get(i) + "@@" + rating.get(j);

	    	        	 context.write(new Text(song_pair), new Text(rating_pair));
	    	        	 System.out.println(song_pair + "  >>>  " +  rating_pair);        		 
	            	 }
	             }
	         }	         	 
	      }
	   }

	   /*
		 * Reducer function 4
		 * input: output <key, value> pairs from Mapper4 -> <song1$$song2	[list of rating1@@rating2] >
		 * output: <key, value> pairs -> <song1$$song2 similarity_value>
		 * For each pair of songs, reducer calculates Jaccard similarity. If this score is greater than 0,reducer writes song-pair & similarity score to output. 
		 */
	   
	   public static class Reduce_JaccardSimilarity extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
	      @Override 
	      public void reduce( Text song_pair,  Iterable<Text > rating_pair_list,  Context context)
	         throws IOException,  InterruptedException {
	    	  
	    	  Vector<Double> rating1 = new Vector<Double>();
	    	  Vector<Double> rating2 = new Vector<Double>();
	    	  
	    	  double dot_product = 0.0;
	    	  double norm1 = 0.0;
	    	  double norm2 = 0.0;
	    	  double j_coeff = 0.0;
	    	          
	    	  for(Text val : rating_pair_list){
	    		  
	    		  String ratings = val.toString();
	    		  
	    		  String[] rating = StringUtils.split(ratings, "@@");
	    		  
	    		  rating1.addElement(Double.parseDouble(rating[0]));
	    		  rating2.addElement(Double.parseDouble(rating[1]));
	    	  }
	    	      	  
	    /*	  Enumeration<Double> e1 = rating1.elements();
	    	  Enumeration<Double> e2 = rating2.elements();
	    	  
	    	  while(e1.hasMoreElements()){
	    		  System.out.print(e1.nextElement() + "	");
	    	  }
	    	  System.out.println();
	    	  while(e2.hasMoreElements()){
	    		  System.out.print(e2.nextElement() + "	");
	    	  }*/
	    	      	  
	    	  if(rating1.size() == rating2.size() && rating1.size() > 1){
	    		  for(int i=0; i<rating1.size(); i++){
	        		  
	    			  dot_product  = dot_product + rating1.get(i) * rating2.get(i);
	    			  
	    			  norm1 = norm1 + Math.pow(rating1.get(i), 2);
	    			  norm2 = norm2 + Math.pow(rating2.get(i), 2); 
	        	  }
	    		  
	    		  j_coeff = dot_product / (norm1 + norm2 - dot_product);
	    		  
	    		  if(j_coeff > 0.5){
	    			  context.write(song_pair, new DoubleWritable(j_coeff));
	            	  System.out.println(song_pair + "	>>>	" + j_coeff);
	    		  }
	    	  }
	      }
	   }
	
	
	   /*
		 * Mapper function 5
		 * input: offset & content (lineText) of every line in the input file: <userID songID=rating_List>
		 * output: <key, value> pairs -> <song1$$song2	rating1@@rating2>
		 * Mapper function reads the input file, one line at a time, converts it to String and splits the line at space & delimiter to extract all the song-rating pairs.
	     * Each song & rating is separated & stored in an ArrayList.
	     * Pairs of songs are formed & corresponding pairs of rating are sent to reducer.
		 */
	   
	   public static class Map_EuclidSimilarity extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
		   
	      public void map( LongWritable offset,  Text lineText,  Context context)
	        throws  IOException,  InterruptedException {
	    	    
	    	  ArrayList<Integer> songs = new ArrayList<Integer>();
	    	  ArrayList<Double> rating = new ArrayList<Double>();
	    	  
	         String line  = lineText.toString();

	         String values_list = StringUtils.substringAfter(line, "	");         
	         String[] song_rating_list = StringUtils.split(values_list, ",");
	         
	         if(song_rating_list.length >1){
	        	 for(String song_rating : song_rating_list){
	            	 
	            	 String[] parts = StringUtils.split(song_rating, "=");
	            	 songs.add(Integer.parseInt(parts[0]));
	            	 rating.add(Double.parseDouble(parts[1]));
	             }
	        	 
	        	 int len = songs.size();
	             
	        	 for(int i=0; i<len-1; i++){
	            	 for(int j=i+1; j<len; j++){
	    	        	 
	    	        	 String song_pair = songs.get(i) + "$$" + songs.get(j);
	    	        	 String rating_pair = rating.get(i) + "@@" + rating.get(j);

	    	        	 context.write(new Text(song_pair), new Text(rating_pair));
	    	        	 System.out.println(song_pair + "  >>>  " +  rating_pair);        		 
	            	 }
	             }
	         }	         	 
	      }
	   }

	   /*
		 * Reducer function 5
		 * input: output <key, value> pairs from Mapper5 -> <song1$$song2	[list of rating1@@rating2] >
		 * output: <key, value> pairs -> <song1$$song2 similarity_value>
		 * For each pair of songs, reducer calculates Euclidean similarity. If this score is greater than 0,reducer writes song-pair & similarity score to output. 
		 */
	   
	   public static class Reduce_EuclidSimilarity extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
	      @Override 
	      public void reduce( Text song_pair,  Iterable<Text > rating_pair_list,  Context context)
	         throws IOException,  InterruptedException {
	    	  
	    	  Vector<Double> rating1 = new Vector<Double>();
	    	  Vector<Double> rating2 = new Vector<Double>();
	    	  
	    	  double sum_diff = 0.0;
	    	  double e_dist = 0.0;
	    	          
	    	  for(Text val : rating_pair_list){
	    		  
	    		  String ratings = val.toString();
	    		  
	    		  String[] rating = StringUtils.split(ratings, "@@");
	    		  
	    		  rating1.addElement(Double.parseDouble(rating[0]));
	    		  rating2.addElement(Double.parseDouble(rating[1]));
	    	  }
	    	      	  
	    /*	  Enumeration<Double> e1 = rating1.elements();
	    	  Enumeration<Double> e2 = rating2.elements();
	    	  
	    	  while(e1.hasMoreElements()){
	    		  System.out.print(e1.nextElement() + "	");
	    	  }
	    	  System.out.println();
	    	  while(e2.hasMoreElements()){
	    		  System.out.print(e2.nextElement() + "	");
	    	  }*/
	    	      	  
	    	  if(rating1.size() == rating2.size() && rating1.size() > 1){
	    		  for(int i=0; i<rating1.size(); i++){
	        		  
	    			  sum_diff = sum_diff + Math.pow((rating1.get(i)-rating2.get(i)), 2);
	        	  }
	    		  
	    		  e_dist = Math.sqrt(sum_diff);
	    		  
	    		  if(e_dist > 0.5){
	    			  context.write(song_pair, new DoubleWritable(e_dist));
	            	  System.out.println(song_pair + "	>>>	" + e_dist);
	    		  }
	    	  }
	      }
	   }
	   /*
	 * Mapper function 6
	 * input: Output of Reducer 4  <song1$$song2 similarity_value>
	 * output: <key, value> pairs -> <song1	song2=similarity>
	 * Mapper function reads the input file, one line at a time, converts it to String and extracts the song-pairs.
	 * Then it splits the pair & writes song1 song2=similarity to output.
	 */

	public static class Map_C_Recommendation extends Mapper<LongWritable ,  Text ,  Text ,  Text > {

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			String line  = lineText.toString();

			String[] parts = StringUtils.split(line);
			if(parts.length == 2){
				String songID_pair = parts[0];
				String rating = parts[1];

				String song1 = StringUtils.substringBefore(songID_pair, "$$");
				String song2 = StringUtils.substringAfter(songID_pair, "$$");

				context.write(new Text(song1), new Text(song2 + "=" + rating));
				System.out.println(song1 + ">>>>" + song2 + "=" + rating);
			}
		}
	}

	/*
	 * Reducer function 6
	 * input: output <key, value> pairs from Mapper6 -><song1	[list of song2=similarity]>
	 * output: <key, value> pairs -> <song1	song2=similarity_for_songs_similar_to_song1>
	 */

	public static class Reduce_C_Recommendation extends Reducer<Text ,  Text ,  Text ,  Text > {
		@Override 
		public void reduce( Text songID,  Iterable<Text > songID_rating_list,  Context context)
				throws IOException,  InterruptedException {

			System.out.println("SongID: " + songID);

			String recommendation = "";

			for(Text item : songID_rating_list){
				System.out.println("Item: " + item);
				recommendation = recommendation + item.toString() + ",";
			}

			recommendation = recommendation.substring(0, recommendation.length()-1);
			context.write(songID, new Text(recommendation));
			System.out.println("RECOMM: >>>" + recommendation);
		}
	}
}