package sds.hadoop.ch05;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
	  protected CompositeKeyComparator() {
	    super(CompositeKey.class, true);
	  }

	  @SuppressWarnings("rawtypes")
	  @Override
	  public int compare(WritableComparable w1, WritableComparable w2) {
	    // 복합키 클래스 캐스팅
	    CompositeKey k1 = (CompositeKey) w1;
	    CompositeKey k2 = (CompositeKey) w2;

	    // 연도 비교
	    int cmp = k1.getItem().compareTo(k2.getItem());
	    if (cmp != 0) {
	      return cmp;
	    }

	    // 월 비교
	    return k1.getDate() == k2.getDate() ? 0 : (k1.getDate() < k2
	      .getDate() ? -1 : 1);
	   }
	}