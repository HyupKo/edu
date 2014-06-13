package sds.hadoop.ch05;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupKeyComparator extends WritableComparator {

	protected GroupKeyComparator() {
		super(CompositeKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeKey k1 = (CompositeKey) w1;
		CompositeKey k2 = (CompositeKey) w2;

		return k1.getItem().compareTo(k2.getItem()); // 항목비교
	}
}
