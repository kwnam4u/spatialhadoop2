package edu.umn.cs.spatialHadoop.star.indexing;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MiFramePartitioner extends Partitioner<MiFrameKey, IntWritable> {

  @Override
  public int getPartition(MiFrameKey mifKey, IntWritable val, int numPartitions ) {
    
    int frameId = mifKey.getFrameId();
    return frameId;
  }
  
  
}
