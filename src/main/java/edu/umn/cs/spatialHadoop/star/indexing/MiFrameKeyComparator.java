package edu.umn.cs.spatialHadoop.star.indexing;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MiFrameKeyComparator extends WritableComparator {
  
  protected MiFrameKeyComparator()
  {
    super( MiFrameKey.class, true);
  }
  
  @SuppressWarnings("rawtypes")
  @Override
  public int compare( WritableComparable w1, WritableComparable w2 )
  {
    MiFrameKey k1 = (MiFrameKey)w1;
    MiFrameKey k2 = (MiFrameKey)w2;
    return k1.getFrameId() - k2.getFrameId();
  }
}
