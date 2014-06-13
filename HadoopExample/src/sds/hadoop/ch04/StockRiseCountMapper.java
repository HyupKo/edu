package sds.hadoop.ch04;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StockRiseCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final static IntWritable outputValue = new IntWritable(1);
	
	private Text outputKey = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(key.get() > 0) {
			String[] columns = value.toString().split(",");
			
			outputKey.set(columns[2].substring(0, 4));
			
			float ricePct = Float.parseFloat(columns[6]) - Float.parseFloat(columns[3]);
			
			if(ricePct > 0) {
				context.write(outputKey, outputValue);
			}
		}
	}

}
