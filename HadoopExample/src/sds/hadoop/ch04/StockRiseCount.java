package sds.hadoop.ch04;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StockRiseCount {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		if (args.length != 2) {
			System.out.println("Usage : WordCount <input> <output>.");
			System.exit(2);
		}
		
		Job job = new Job(conf, "WordCount");
		
		// A class using job.
		job.setJarByClass(StockRiseCount.class);
		
		// Setting mapper and reducer.
		job.setMapperClass(StockRiseCountMapper.class);
		job.setReducerClass(StockRiseCountReducer.class);
		
		// Setting inoutput format.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Setting output key/value class.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// Setting file in, out path.
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// Run.
		job.waitForCompletion(true);
	}

}
