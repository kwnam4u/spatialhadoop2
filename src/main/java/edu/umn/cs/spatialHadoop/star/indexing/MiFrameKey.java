package edu.umn.cs.spatialHadoop.star.indexing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MiFrameKey implements WritableComparable<MiFrameKey> {

  private int frameId;
  private int indexId;
  

  public MiFrameKey( int indexFrameId, int microIndexId )
  {
    this.frameId = indexFrameId;
    this.indexId = microIndexId;
  }
  
  public int getFrameId() {
    return frameId;
  }

  public int getIndexId() {
    return indexId;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt( this.frameId );
    out.writeByte( this.indexId );    
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.frameId = in.readInt();
    this.indexId = in.readInt();
  }

  @Override
  public int compareTo(MiFrameKey key) {
    int result;
    
    result = frameId - key.frameId;
    if ( result == 0 )
      result = indexId - key.indexId;
        
    return result;
  }
  
}
