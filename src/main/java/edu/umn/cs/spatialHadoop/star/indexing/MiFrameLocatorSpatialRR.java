package edu.umn.cs.spatialHadoop.star.indexing;

import java.util.ArrayList;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.indexing.Partitioner;

public class MiFrameLocatorSpatialRR extends MiFrameLocator {
  protected int replicationFactor = 3;
  
  public MiFrameLocatorSpatialRR(Partitioner geoPartitioner, int numPartition ) {
    super(geoPartitioner, numPartition);
  }
  
  public MiFrameLocatorSpatialRR(Partitioner geoPartitioner, int numPartition, int replicationFactor ) {
    super(geoPartitioner, numPartition);
    this.replicationFactor = replicationFactor;
  }

  @SuppressWarnings("unchecked")
  @Override
  public int[] placement( CellInfo[] grid, int numFrames ) 
  {
    int[] indexToFrame = null;
    ArrayList<Integer>[] cellPerFrame = null;    
    
    assert( numFrames > 0 && numFrames <= grid.length );          
    
    cellPerFrame = (ArrayList<Integer>[])new ArrayList[ numFrames ];   

    // Initial setting :
    int rrFrameNumber = 0;
    int minCellId = -1;
    CellInfo previousCell = null;

    CellInfo[] unassignedCells = grid.clone();    
    
    minCellId = 0;
    cellPerFrame[rrFrameNumber++].add( 0 );
    previousCell = unassignedCells[ 0 ] ;
    unassignedCells[ 0 ] = null;
    
    // Find minimum distance cell from previous assigned cell on unassigned cell array. 
    for( int i = 1; i < unassignedCells.length -1 ; i++ )
    {
      minCellId = getMinDistanceCell(  previousCell, unassignedCells );
      
      if ( minCellId <= 0 )
        break;
      
      cellPerFrame[ rrFrameNumber%numFrames ].add( minCellId );
      previousCell = unassignedCells[minCellId];
      unassignedCells[minCellId] = null;
      rrFrameNumber++;
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
