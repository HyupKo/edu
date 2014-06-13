package sds.hadoop.ch04;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StockRiseCountComb {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		conf.setBoolean("mapred.compress.map.output", true);
		conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		
		if (args.length != 2) {
			System.out.println("Usage : WordCount <input> <output>.");
			System.exit(2);
		}
		
		Job job = new Job(conf, "WordCount");
		
		// A class using job.
		job.setJarByClass(StockRiseCountComb.class);
		
		// Setting mapper and reducer.
		job.setMapperClass(StockRiseCountMapper.class);
		job.setReducerClass(StockRiseCountReducer.class);
		
		// Use Combiner Class.
		// job.setCombinerClass(StockRiseCountReducer.class);
		
		// Setting inoutput format.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Setting output key/value class.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// Setting file in, out path.
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		
		// Run.
		job.waitForCompletion(true);
	}

}
