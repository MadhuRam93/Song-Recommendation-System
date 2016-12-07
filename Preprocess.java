package org.myorg;

import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class Preprocess {

	/*
	 * Mapper function 1
	 * input: offset & content (lineText) of every line in the input file
	 * output: <key, value> pairs -> <userID	songID=rating>
	 * Mapper function reads the input file, one line at a time, converts it to String and splits the line at every space to extract all the words - userID, songID & rating
     * Each line is written to the context in the form : userID	songID=rating
	 */

	public static class Map_Preprocess extends Mapper<LongWritable ,  Text ,  Text ,  Text > {

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			String line  = lineText.toString();

			String[] parts = StringUtils.split(line);
			if(parts.length == 3){
				String userID = parts[0];
				String songID = parts[1];
				String rating = parts[2];

				context.write(new Text(userID), new Text(songID + "=" + rating));
			}
		}
	}

	/*
	 * Reducer function 1
	 * input: output <key, value> pairs from Mapper1 -> <userID	[list of songID=rating]>
	 * output: <key, value> pairs -> <userID songID=rating_List>	
	 * Reducer function iterates over the list of song-rating for each user, appends them to a string & outputs a string for each user.
	 */

	public static class Reduce_Preprocess extends Reducer<Text ,  Text ,  Text ,  Text > {
		@Override 
		public void reduce( Text userID,  Iterable<Text > values,  Context context)
				throws IOException,  InterruptedException {

			String songID_rating = "";
			for ( Text val  : values) {
				songID_rating = songID_rating + "," + val.toString();
			}

			context.write(userID,  new Text(songID_rating));
		}
	}


}
