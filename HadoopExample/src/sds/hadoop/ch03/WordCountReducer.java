package sds.hadoop.ch03;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer.
 * @author hadoop
 *
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private IntWritable result = new IntWritable();
	
	/**
	 * Function reduce.
	 * 
	 * @param key
	 * @param values
	 * @param context
	 */
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		
		for (IntWritable val : values) {
			sum += val.get();
		}
		
		result.set(sum);
		context.write(key, result);
	}

}
