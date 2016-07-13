package edu.umn.cs.spatialHadoop.star.indexing;

import java.util.ArrayList;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.indexing.Partitioner;

public class MiFrameLocatorRoundRobin extends MiFrameLocator {

  public MiFrameLocatorRoundRobin(Partitioner geoPartitioner, int numPartition) {
    super(geoPartitioner, numPartition);
    // TODO Auto-generated constructor stub
  }

  @SuppressWarnings("unchecked")
  @Override
  public int[] placement( CellInfo[] grid, int numFrames ) 
  {
    int[] indexToFrame = null;
    ArrayList<Integer>[] cellPerFrame = null;    
    
    assert( numFrames > 0 && numFrames <= grid.length );
    
    // Assign each cell to Frame by round robin 
    cellPerFrame = (ArrayList<Integer>[])new ArrayList[ numFrames ];
    for( int i=0; i < grid.length ; i++ )
    {
      cellPerFrame[ i%numFrames ].add( i );
    }
      
    // Make IndexToFrame Array by cellPerFrame
    indexToFrame = new int[ grid.length ];
    for( int i=0; i < cellPerFrame.length ; i++ )
    {
      for( int cell : cellPerFrame[i] )
        indexToFrame[cell] = i ;
    }
    
    return indexToFrame;
  }

}
