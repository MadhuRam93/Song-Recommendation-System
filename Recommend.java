package org.myorg;

import java.io.IOException;
import java.io.*;
import java.util.*;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map.Entry;
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
import org.apache.hadoop.io.Text;

public class Recommend extends Configured implements Tool {

      public static void main( String[] args) throws  Exception {
      		int res  = ToolRunner .run( new Recommend(), args);
      		System .exit(res);
   	}

	/*run starts*/
   	public int run( String[] args) throws  Exception {
	   
	   	Configuration conf = new Configuration();
	  	Path output_path = new Path(args[2]);
 		conf.set("Sim_Path", args[1]);
		FileSystem hdfs = FileSystem.get(conf);
	   
	   	try {
		   	if(hdfs.exists(output_path)){
			   	hdfs.delete(output_path, true);
		   	} 
		}
		catch (IOException e) {
				e.printStackTrace();
		}
	   
      	Job job = Job .getInstance(conf, "Recommend");
      	job.setJarByClass(Recommend.class);
      
      	FileInputFormat.addInputPaths(job,  args[0]);
      	FileOutputFormat.setOutputPath(job, output_path);
      
      	job.setMapperClass( Map_Recom .class);
      	job.setReducerClass( Reduce_Recom .class);
      
      	job.setOutputKeyClass( Text .class);
      	job.setOutputValueClass( Text .class);

      	int success =  job.waitForCompletion( true)  ? 0 : 1;
	return 0;
	}/*run ends */

   
	//Input: test_0.txt 
	//Output: <UserID	SongID,Rating>
   public static class Map_Recom extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
	   
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
	//Input: < UserID	<Listof SongID,Rating>>
	//Output: < UserID	<List of Recommendations >
    public static class Reduce_Recom extends Reducer<Text, Text, Text, Text> {
		/*HashMap to store the similarity list and value for songs*/
		private Map<Integer, String> ISMap = new HashMap<Integer, String>();/* itemId1	 <itemId2    rating>*/

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			Map<Integer,Integer> UserList = new HashMap<Integer,Integer>();//list of songs user has listened to from train data
			Map<Integer,Float> recom = new HashMap<Integer,Float>();
			
			for (Text value : values){
				int i = Integer.parseInt(value.toString().split(",")[0].trim());
				int r = Integer.parseInt(value.toString().split(",")[1].trim());
				UserList.put(i,r);
			}
			
			/*For each Song from the list of songs User has liked, add the list of similar songs to recommendation */
			for (int itemId : UserList.keySet()) {
				
				int user_song = itemId;
				boolean flag = false;
				String sim_list = "";
				if( ISMap.containsKey(user_song)){
					sim_list = ISMap.get(user_song);
					flag = true;
				}
				if(flag){
					String[] a = sim_list.split(","); 
					/*sort the similarity list*/
    					Arrays.sort(a, new Comparator<String>() { 
     						 public int compare(String str1, String str2) {
          					 String substr1 = str1.substring(str1.lastIndexOf("=")+1);
         					 String substr2 = str2.substring(str2.lastIndexOf("=")+1);

          					return Double.valueOf(substr2).compareTo(Double.valueOf(substr1));}});
					
					for(int i=0; i< a.length ; i++)
					{
						int song = Integer.parseInt(a[i].split("=")[0]);
						float sim = Float.parseFloat(a[i].split("=")[1]);
						float p = predictRating(song, UserList);//Predicting the rating for similar song
						
						if( recom.containsKey(song))
						{	
							if( p >= recom.get(song))
								recom.put(song,p);
							else
								recom.put(song,recom.get(song));
						}
					}
				}
				recom.remove(user_song);//Remove the songs user has already listed/rated from the recommendation list.
			}
		
			Set<Entry<Integer, Float>> set = recom.entrySet();
    		List<Entry<Integer, Float>> list = new ArrayList<Entry<Integer, Float>>(set);
    		Collections.sort( list, new Comparator<Map.Entry<Integer, Float>>()
        	{
					public int compare( Map.Entry<Integer, Float> o1, Map.Entry<Integer, Float> o2 )
					{
						return (o2.getValue()).compareTo( o1.getValue() );
					}} );
			String recommendations = "";
			int limit = 0;
			for(Map.Entry<Integer, Float> entry:list){
				limit++;
				recommendations += Integer.toString(entry.getKey()) + ",";
				if( limit > 10)
					break;
    		} 
			recommendations = recommendations.substring(0, recommendations.length()-1);
			context.write(key, new Text(recommendations));
		}/*reduce ends*/

		/*setup(org.apache.hadoop.mapreduce.Mapper.Context context) Called once at the beginning of the task.*/
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			Configuration confR = context.getConfiguration();
			String simFileName = confR.get("Sim_Path");
			loadSimilarityData(simFileName);
		}
		/* predict the rating using weighted average */
		private float predictRating(int itemId, Map<Integer,Integer> userlist){
			float sumSR = 0, sumS = 0;

			for (Integer item : userlist.keySet()) {

				//System.out.println("ratings >> " +sr);
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

				if (ISMap.containsKey(smallerId)){
					String[] t = ISMap.get(smallerId).split(",");
    					Map<Integer, Float> smap = new HashMap<Integer, Float>();
    					for( String str:t){
      						int i = Integer.parseInt(str.split("=")[0]);
      						float f = Float.parseFloat(str.split("=")[1]);
      						smap.put(i,f);
					}
					if(smap.containsKey(largerId))
						similarity = smap.get(largerId);
				 
					else 
						similarity = 0; // handle the case where item1 and item2 are not similar
				}
				
				sumSR += similarity * rating;
				sumS += similarity;
			}
			float pr = 0;
			if(sumS > 0)
				pr = sumSR/sumS;

			return pr;
		}

		/* Load data from part-r-00000 into Item-Similarity Map : ISMap */
		private void loadSimilarityData( String simFileName) throws FileNotFoundException, IOException {
			
			Path pt=new Path(simFileName);
                        FileSystem fs = FileSystem.get(new Configuration());
                        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] tokens = line.split("\t");
				int item1 = Integer.parseInt(tokens[0]);
		
				ISMap.put(item1, tokens[1]);
			}
			br.close();
		}
	}
}