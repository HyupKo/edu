package sds.hadoop.ch04;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class StockCountMultipleOutputsReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	private MultipleOutputs<Text, IntWritable> mos;

	// reduce 출력키
	private Text outputKey = new Text();
	// reduce 출력값
	private IntWritable result = new IntWritable();

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<Text, IntWritable>(context);
	}

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		// 콤마 구분자 분리
		String[] colums = key.toString().split(",");

		// 출력키 설정
		outputKey.set(colums[1]);

		// NASDAQ 데이터
		if (colums[0].equals("Q")) {
			// 데이터 합산 합산
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			// 출력값 설정
			result.set(sum);
			// 출력 데이터 생성
			mos.write("nasdaq", outputKey, result);
			// NYSE 함산
		} else {
			// 데이터 합산
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			// 출력값 설정
			result.set(sum);
			// 출력 데이터 생성
			mos.write("nyse", outputKey, result);
		}
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}
}
