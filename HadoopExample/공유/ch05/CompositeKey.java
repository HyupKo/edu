package sds.hadoop.ch05;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKey implements WritableComparable<CompositeKey> {
	private String item;
	private Integer date;

	public CompositeKey() {
	}

	public CompositeKey(String item, Integer date) {
		this.item = item;
		this.date = date;
	}

	public String getItem() {
		return item;
	}

	public void setItem(String item) {
		this.item = item;
	}

	public Integer getDate() {
		return date;
	}

	public void setDate(Integer date) {
		this.date = date;
	}

	@Override
	public String toString() {
		return (new StringBuilder()).append(item).append(":").append(date)
				.toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		item = WritableUtils.readString(in);
		date = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, item);
		out.writeInt(date);
	}

	@Override
	public int compareTo(CompositeKey key) {
		int result = item.compareTo(key.item);
		if (0 == result) {
			result = date.compareTo(key.date);
		}
		return result;
	}
}
