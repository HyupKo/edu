package sds.hadoop.ch05;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import sds.hadoop.ch04.StockCounters;

public class StockCountMapperWithCompositeKey extends
		Mapper<LongWritable, Text, CompositeKey, IntWritable> {

	// map 출력값
	private final static IntWritable outputValue = new IntWritable(1);
	// map 출력키
	private CompositeKey outputKey = new CompositeKey();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		if (key.get() > 0) {
			// 구분자로 데이터 분리
			String[] columns = value.toString().split(",");
			if (columns != null && columns.length > 0) {
				try {
					// NASDAQ 데이터 분석
					if (columns[0].equals("NASDAQ")) {
						float price = Float.parseFloat(columns[6])
								- Float.parseFloat(columns[3]);

						if (price > 0) {
							// 출력키 설정
							outputKey.setItem("Q," + columns[1]);
							outputKey.setDate(new Integer(columns[2]
									.replaceAll("-", "").substring(0, 6)));
							// 출력 데이터 설ㅈ렁
							context.write(outputKey, outputValue);
						} else if (price == 0) {
							context.getCounter(StockCounters.nasdaq_price_same)
									.increment(1);
						} else if (price < 0) {
							context.getCounter(StockCounters.nasdaq_price_fall)
									.increment(1);
						}
					} else {
						context.getCounter(StockCounters.nasdaq_price_na)
								.increment(1);
					}

					if (columns[0].equals("NYSE")) {
						float price = Float.parseFloat(columns[6])
								- Float.parseFloat(columns[3]);

						if (price > 0) {
							// NYSE 데이터 분석
							outputKey.setItem("E," + columns[1]);
							outputKey.setDate(new Integer(columns[2]
									.replaceAll("-", "").substring(0, 6)));
							// 출력 데이터 생성
							context.write(outputKey, outputValue);
						} else if (price == 0) {
							context.getCounter(StockCounters.nyse_price_same)
									.increment(1);
						} else if (price < 0) {
							context.getCounter(StockCounters.nyse_price_fall)
									.increment(1);
						}
					} else {
						context.getCounter(StockCounters.nyse_price_na)
								.increment(1);
					}

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
}
