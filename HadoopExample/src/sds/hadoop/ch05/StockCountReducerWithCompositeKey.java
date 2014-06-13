package sds.hadoop.ch05;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class StockCountReducerWithCompositeKey extends
		Reducer<CompositeKey, IntWritable, CompositeKey, IntWritable> {

	private MultipleOutputs<CompositeKey, IntWritable> mos;

	// reduce 출력키
	private CompositeKey outputKey = new CompositeKey();

	// reduce 출력값
	private IntWritable result = new IntWritable();

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<CompositeKey, IntWritable>(context);
	}

	public void reduce(CompositeKey key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		// 구분자로 데이터 분리
		String[] columns = key.getItem().split(",");

		int sum = 0;
		Integer bDate = key.getDate();

		if (columns[0].equals("Q")) {
			for (IntWritable value : values) {
				if (!bDate.equals(key.getDate())) {
					result.set(sum);
					outputKey.setItem(key.getItem().substring(2));
					outputKey.setDate(bDate);
					mos.write("nasdaq", outputKey, result);
					sum = 0;
				}
				sum += value.get();
				bDate = key.getDate();
			}
		} else {
			for (IntWritable value : values) {
				if (!bDate.equals(key.getDate())) {
					result.set(sum);
					outputKey.setItem(key.getItem().substring(2));
					outputKey.setDate(bDate);
					mos.write("nyse", outputKey, result);
					sum = 0;
				}
				sum += value.get();
				bDate = key.getDate();
			}
		}
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}
}
