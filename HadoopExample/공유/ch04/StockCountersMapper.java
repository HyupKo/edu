package sds.hadoop.ch04;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StockCountersMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	private String workType;
	// map 출력값
	private final static IntWritable outputValue = new IntWritable(1);

	// map 출력키
	private Text outputKey = new Text();

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		workType = context.getConfiguration().get("workType");
	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		if (key.get() > 0) {
			// 콤마 구분자 분리
			String[] columns = value.toString().split(",");
			if (columns != null && columns.length > 0) {
				try {
					// NASDAQ 데이터 출력
					if (workType.equals("nasdaq")) {
						// NASDAQ 데이터 출력
						if (columns[0].equals("NASDAQ")) {
							float price = Float.parseFloat(columns[6])
									- Float.parseFloat(columns[3]);

							if (price > 0) {
								// 출력키 설정
								outputKey
										.set("Q," + columns[2].substring(0, 4));
								// 출력 데이터 설정
								context.write(outputKey, outputValue);
							} else if (price == 0) {
								context.getCounter(
										StockCounters.nasdaq_price_same)
										.increment(1);
							} else if (price < 0) {
								context.getCounter(
										StockCounters.nasdaq_price_fall)
										.increment(1);
							}
						} else {
							context.getCounter(StockCounters.nasdaq_price_na)
									.increment(1);
						}
						// NYSE 데이터 출력
					} else if (workType.equals("nyse")) {
						if (columns[0].equals("NYSE")) {
							float price = Float.parseFloat(columns[6])
									- Float.parseFloat(columns[3]);

							if (price > 0) {
								// 출력키 설정
								outputKey
										.set("E," + columns[2].substring(0, 4));
								// 출력 데이터 설정
								context.write(outputKey, outputValue);
							} else if (price == 0) {
								context.getCounter(
										StockCounters.nyse_price_same)
										.increment(1);
							} else if (price < 0) {
								context.getCounter(
										StockCounters.nyse_price_fall)
										.increment(1);
							}
						} else {
							context.getCounter(StockCounters.nyse_price_na)
									.increment(1);
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
}
