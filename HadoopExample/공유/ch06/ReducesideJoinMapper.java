package sds.hadoop.ch06;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReducesideJoinMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	// 태그 선언
	public final static String DATA_TAG = "B";

	// map 출력키 생성
	private Text outputKey = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		if (key.get() > 0) {
			// 구분자로 데이터 분리
			String[] colums = value.toString().split(",");
			if (colums != null && colums.length > 0) {
				try {
					outputKey.set(colums[1] + "_" + DATA_TAG);
					context.write(outputKey, value);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
}
