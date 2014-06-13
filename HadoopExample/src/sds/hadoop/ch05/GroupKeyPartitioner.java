package sds.hadoop.ch05;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class GroupKeyPartitioner extends Partitioner<CompositeKey, IntWritable> {
	@Override
	public int getPartition(CompositeKey key, IntWritable val, int numPartitions) {
		int hash = key.getItem().hashCode();
		int partition = hash % numPartitions;
		System.out.println(partition);
		return partition;
	}
}