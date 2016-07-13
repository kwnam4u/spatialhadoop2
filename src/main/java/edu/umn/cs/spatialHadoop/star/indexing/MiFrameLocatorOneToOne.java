package edu.umn.cs.spatialHadoop.star.indexing;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.indexing.Partitioner;

public class MiFrameLocatorOneToOne extends MiFrameLocator {

  public MiFrameLocatorOneToOne( Partitioner geoPartitioner, int numPartition) {
    super(geoPartitioner, numPartition);
    // TODO Auto-generated constructor stub
  }

  @Override
  public int[] placement( CellInfo[] grid, int numFrames ) 
  {
    int[] indexToFrame = null;
    
    assert( numFrames > 0 && grid.length == numFrames );
    
    indexToFrame = new int[ grid.length ];
    for( int i=0; i < grid.length ; i++ )
      indexToFrame[i] = i;
    
    return indexToFrame;
  }

}
