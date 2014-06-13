package sds.hadoop.ch05;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StockCountWithCompositeKey extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(getConf(), args)
				.getRemainingArgs();
		// 입력출 데이터 경로 확인
		if (otherArgs.length != 2) {
			System.err.println("Usage: StockCountWithCompositeKey <in> <out>");
			System.exit(2);
		}
		// Job 이름 설정
		Job job = new Job(getConf(), "StockCountWithCompositeKey");
		job.setNumReduceTasks(10);

		// 입출력 데이터 경로 설정
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Job 클래스 설정
		job.setJarByClass(StockCountWithCompositeKey.class);
		job.setPartitionerClass(GroupKeyPartitioner.class);
		job.setGroupingComparatorClass(GroupKeyComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);

		// Mapper 클래스 설정
		job.setMapperClass(StockCountMapperWithCompositeKey.class);
		// Reducer 클래스 설정
		job.setReducerClass(StockCountReducerWithCompositeKey.class);
		// 콤바이너 클래스 설정
		job.setCombinerClass(StockCountReducerWithCompositeKey.class);

		job.setMapOutputKeyClass(CompositeKey.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 입출력 데이터 포맷 설정
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		// 출력키 및 출력값 유형 설정
		job.setOutputKeyClass(CompositeKey.class);
		job.setOutputValueClass(IntWritable.class);

		// MultipleOutputs 설정
		MultipleOutputs.addNamedOutput(job, "nasdaq", TextOutputFormat.class,
				CompositeKey.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "nyse", TextOutputFormat.class,
				CompositeKey.class, IntWritable.class);
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// Tool 인터페이스 실행
		int res = ToolRunner.run(new Configuration(),
				new StockCountWithCompositeKey(), args);
		System.out.println("## RESULT:" + res);
	}
}