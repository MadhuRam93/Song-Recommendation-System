package org.myorg;

import java.io.IOException;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Enumeration;
import java.util.Vector;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.io.FileNotFoundException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Predict extends Configured implements Tool {

      public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new Predict(), args);
      System .exit(res);
   	}

   	public int run( String[] args) throws  Exception {
	   
	   	Configuration conf = new Configuration();
	   
		FileSystem hdfs = FileSystem.get(conf);
	   	String intermediate_dir = "intermediate_dir";
	   	Path output_path = new Path(args[3]);
	   	Path intermediate_path = new Path(intermediate_dir);
	   	try {
		   	if(hdfs.exists(output_path)){
			   hdfs.delete(output_path, true);
		   } if(hdfs.exists(intermediate_path)){
			   hdfs.delete(intermediate_path, true);
		   }				
		} catch (IOException e) {
				e.printStackTrace();
		}
	   
	   	conf.set("Sim_Path", args[1]);
	   	conf.set("test_Path", args[2]);
      	Job job = Job .getInstance(conf, "Predict");
      	job.setJarByClass(Predict.class);

	Path intermed1 = new Path(intermediate_path, "intermed1");
      
      	FileInputFormat.addInputPaths(job,  args[0]);
      	FileOutputFormat.setOutputPath(job, intermed1);
      
      	job.setMapperClass( Map_Predict .class);
      	job.setReducerClass( Reduce_Predict .class);
		job.setNumReduceTasks(1);
  
      	int success =  job.waitForCompletion( true)  ? 0 : 1;
		if(success == 0){
    	  
    	  	Configuration conf2 = new Configuration();
    	  
         	Job job2  = Job .getInstance(conf2, "Accuracy");
         	job2.setJarByClass(Predict.class);
         
         	Path intermed1_output = new Path(intermed1, "part-r-00000");

         	FileInputFormat.addInputPath(job2,  intermed1_output);
         	FileOutputFormat.setOutputPath(job2, output_path);
         
         	job2.setMapperClass( Map_Accuracy .class);
      		job2.setReducerClass( Reduce_Accuracy .class);

		int success2 =  job2.waitForCompletion( true)  ? 0 : 1;
	}
	return 0;
	}

	/* input:  (UserId, [(itemId,Rating)])
	* output: (UserId, ItemID, ActualRating, PredictedRating)
	*/

//takes test_* file as input
   public static class Map_Predict extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
	   
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
    	    
         String line  = lineText.toString();

         String[] parts = StringUtils.split(line);
         if(parts.length == 3){
        	 String userID = parts[0];
        	 String songID = parts[1];
        	 String rating = parts[2];
        	 
        	 context.write(new Text(userID), new Text(songID + "," + rating));
         }
      }
   }

	public static class Reduce_Predict extends Reducer<Text, Text, Text, Text> {
		//private String testFile = "/user/cloudera/recmdation/input/test_0.txt";//Testfile test_0.txt
		//private String simFileName = "/user/cloudera/recmdation/input/test0_sim.txt";//Read it from parameter!!mxd
		private Map<Integer, Map<Integer, Float>> ISMap = new HashMap<Integer, Map<Integer, Float>>();
		private Map<String, List<String>> testList = new HashMap<String, List<String>>();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			Map<Integer,Integer> UserList = new HashMap<Integer,Integer>();//list of songs user has listened to from train data
			
			for (Text value : values){
				int i = Integer.parseInt(value.toString().split(",")[0].trim());
				int r = Integer.parseInt(value.toString().split(",")[1].trim());
				UserList.put(i,r);
			}

			for (String val : testList.get(key.toString())) {
				
				String itemId = val.split(",")[0];
				String rating = val.split(",")[1];
			
				int p = Math.round(predictRating(Integer.parseInt(itemId.trim()), UserList));

				Text Okey = new Text(key.toString() + "\t" + itemId);
				Text Ovalue = new Text(rating + "\t" + Integer.toString(p));

				context.write(Okey, Ovalue); // UID	SongID	ActualRating	PredRating
			}
		}/*reduce ends*/

		/*setup(org.apache.hadoop.mapreduce.Mapper.Context context) Called once at the beginning of the task.*/
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			Configuration confR = context.getConfiguration();
			String simFileName = confR.get("Sim_Path");
			String testFilePath = confR.get("test_Path");
			loadTestData(testFilePath);
			loadSimilarityData(simFileName);
		}

		/* predict the rating using weighted average */
		private float predictRating(int itemId, Map<Integer,Integer> userlist){
			float sumSR = 0, sumS = 0;

			for (Integer item : userlist.keySet()) {

				float similarity=0;
				int smallerId = item;
				int largerId = itemId;
				int rating = userlist.get(item);
				System.out.println("id and rating >> " + item+ " , " + rating);
				if( itemId < smallerId){
					int t = largerId;
					largerId = smallerId;
					smallerId = t;
				}

				if (ISMap.containsKey(smallerId) && ISMap.get(smallerId).containsKey(largerId)) {
					similarity = ISMap.get(smallerId).get(largerId);
				} 
				else {
					similarity = 0; // handle the case where item1 and item2 are not similar
				}
				System.out.println("sim for "+itemId+"is" + similarity);
				sumSR += similarity * rating;
				sumS += similarity;
			}
			float pr = 0;
			if(sumS > 0)
				pr = sumSR/sumS;

			return pr;
		}
		
		/* initialize the testing data. Load all data from test_0.txt into testList */
		private void loadTestData(String testFilePath) throws FileNotFoundException, IOException {
			
			Path pt=new Path(testFilePath);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line = null;
	
			while ((line = br.readLine()) != null) {
				String[] tokens = line.split("\t");
				String userId = tokens[0];
				String itemId = tokens[1];
				String rating = tokens[2];
				String SR = itemId + "," + rating;
		
				if (testList.containsKey(userId)) {
					testList.get(userId).add(SR);
				} 
				else {
					List<String> temp = new ArrayList<String>();
					temp.add(SR);
					testList.put(userId, temp);
				}
			}
			br.close();
		}/*ends*/

		/* Load data from part-r-00000 into Item-Similarity Map : ISMap */
		private void loadSimilarityData(String simFileName) throws FileNotFoundException, IOException {
			/* item1	(list of [item=sim]) */
			Path pt=new Path(simFileName);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] tokens = line.split("\t");
				int item1 = Integer.parseInt(tokens[0]);
				String[] item_sim_list = tokens[1].split(","); //"item=sim"[]
				int i = item_sim_list.length;
				Map<Integer, Float> temp = new HashMap<Integer, Float>();
				for( int m =0; m<i;m++)
				{
					String item_2 = item_sim_list[m].split("=")[0];
					Float sim = Float.parseFloat(item_sim_list[m].split("=")[1]);
					int item2 = Integer.parseInt(item_2);
			
					if (item1 > item2) {
						int t = item1;
						item1 = item2;
						item2 = t;
					}
					temp.put(item2, sim);	
				}
				ISMap.put(item1, temp);
			}
			br.close();
		}
	}

public static class Map_Accuracy extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
	   
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
    	    
         String line  = lineText.toString();

         String[] parts = StringUtils.split(line);
         if(parts.length == 4){
        	 String key = parts[0]+parts[1];
        	 int Arating = Integer.parseInt(parts[2].trim());
        	 int Prating = Integer.parseInt(parts[3].trim());
        	 
        	 context.write(new Text("Error_Count"), new IntWritable(Math.abs(Arating - Prating)));
         }
      }
   }

	public static class Reduce_Accuracy extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			int total = 0;
			double sum = 0;
			for (IntWritable val : values) {
				total++;
				sum += val.get();
			}
			double accuracy = sum/total;
				context.write(new Text("Accuracy : "), new DoubleWritable(accuracy)); 
			}
		}/*reduce ends*/

}