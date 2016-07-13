package edu.umn.cs.spatialHadoop.star.indexing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.indexing.Partitioner;

public class MiFrameLocatorMaxDistance extends MiFrameLocator {
  protected int replicationFactor = 3;
  
  public MiFrameLocatorMaxDistance(Partitioner geoPartitioner, int numPartition ) {
    super(geoPartitioner, numPartition);
  }
  
  public MiFrameLocatorMaxDistance(Partitioner geoPartitioner, int numPartition, int replicationFactor ) {
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
    
    int cellId = 0;
    cellPerFrame = (ArrayList<Integer>[])new ArrayList[ numFrames ];   

    // Initial setting : 
    for( cellId = 0; cellId < numFrames; cellId++ )
      cellPerFrame[cellId].add( cellId );

    //    
    int maxDistFrame = 0;
    double[] distances = new double[numFrames];
    for( ; cellId < grid.length; cellId++)
    {
      for( int j=0; j < cellPerFrame.length; j++ )
      {
        distances[j] = getMinDistance( cellPerFrame[j], grid[cellId], grid  );
      }
      
      maxDistFrame = getMaxDistanceIdx( distances );
      cellPerFrame[maxDistFrame].add( cellId );
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
  
  protected double getMinDistance(  ArrayList<Integer> cellsInFrame, CellInfo cell, CellInfo[] grid )
  {
    double minDistance = Double.MAX_VALUE;
    
    for( int cellId : cellsInFrame )
    {
      minDistance = getMinDistance( grid[ cellId ], cell ); 
    }
    
    return minDistance;
  }
}
