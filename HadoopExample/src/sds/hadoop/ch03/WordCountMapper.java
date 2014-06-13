package sds.hadoop.ch03;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper Class.
 * @author hadoop
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
	/**
	 * Function Map.
	 * 
	 * @param key
	 * @param value
	 * @param context
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		
		while (itr.hasMoreElements()) {
			word.set(itr.nextToken());
			context.write(word, one);
		}
		
		// LongWritable key : not use.
	}

}
