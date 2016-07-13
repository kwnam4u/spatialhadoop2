package edu.umn.cs.spatialHadoop.star.indexing;

import java.util.ArrayList;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.indexing.Partitioner;

public class MiFrameLocatorSpatialBurst extends MiFrameLocator {
  protected int burstFactor = 3;
  
  public MiFrameLocatorSpatialBurst(Partitioner geoPartitioner, int numPartition ) {
    super(geoPartitioner, numPartition);
  }
  
  public MiFrameLocatorSpatialBurst(Partitioner geoPartitioner, int numPartition, int burstFactor ) {
    super(geoPartitioner, numPartition);
    this.burstFactor = burstFactor;
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
    unassignedCells[0] = null;
    
    numFrames = (int) Math.ceil(grid.length / this.burstFactor );
    
    for( int i = 0 ; i < numFrames ; i++ )
    {
      for( int j = 0 ; j < burstFactor-1; j++ )
      {
        minCellId = getMinDistanceCell( previousCell, unassignedCells );
        
        if ( minCellId <= 0 )
          break;
        
        cellPerFrame[ rrFrameNumber ].add( minCellId );
        
        previousCell = unassignedCells[minCellId];
        unassignedCells[minCellId] = null;
      }
      
      if ( minCellId <= 0 )
        break;
      
      rrFrameNumber = (++rrFrameNumber)%numFrames;
    }        
      
    unassignedCellRRProcess( cellPerFrame, unassignedCells, rrFrameNumber, numFrames );
    
    // Make IndexToFrame Array by cellPerFrame
    indexToFrame = new int[ grid.length ];
    for( int i=0; i < cellPerFrame.length ; i++ )
    {
      for( int cell : cellPerFrame[i] )
        indexToFrame[cell] = i ;
    }
    
    return indexToFrame;
  }

  public void unassignedCellRRProcess( ArrayList[] cellPerFrame, CellInfo[] unassignedCells, 
      int rrFrameNumber, int numFrames  )
  {
    int count = 0;
    
    for( int i = 0 ; i < unassignedCells.length; i++ )
    {
      if ( unassignedCells[i] != null )
      {
        cellPerFrame[ rrFrameNumber++%numFrames ].add(i);
      }
    }
      
  }
}
