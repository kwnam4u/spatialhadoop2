package edu.umn.cs.spatialHadoop.star.indexing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.mortbay.log.Log;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.indexing.Partitioner;

public abstract class MiFrameLocator implements Writable  {
  /**Configuration line for partitioner class*/
  private static final String LOCATORCLASS = "Locator.Class";
  private static final String LOCATORVALUE = "Locator.Value";

  CellInfo[] cellGrid = null;
  int[] indexToFrameMap = null;
  int numPartitions = 0;
  
  @SuppressWarnings("unchecked")
  public MiFrameLocator( Partitioner geoPartitioner, int numPartitions )
  {
     int c = 0;
     CellInfo cell = null;
    
    assert( geoPartitioner != null );    
    
    this.numPartitions = numPartitions;
    c = geoPartitioner.getPartitionCount();

    assert( c > 0 );
    
    this.cellGrid = new CellInfo[ c ];
    for( int i=0 ; i < c; i++ )
    {
      cell = geoPartitioner.getPartitionAt(i);
      this.cellGrid[i]= cell ;
    }
    
    this.indexToFrameMap = placement( cellGrid, numPartitions );
  }
  
  public abstract int[] placement( CellInfo[] grid, int numFrames );
  
  public MiFrameKey overlapPartitions( Shape shape )
  {
    Rectangle rectShape = null;
    
    rectShape = shape.getMBR();    
    for( int i = 0; i < cellGrid.length ; i++ )
    {
      if ( rectShape.x2 > cellGrid[i].x1 && cellGrid[i].x2 > rectShape.x1 
            && rectShape.y2 > cellGrid[i].y1 && cellGrid[i].y2 > rectShape.y1 )
        return new MiFrameKey( indexToFrameMap[i], i );
    }
    
    return null;
  }
  
  public void overlapPartitions(Shape shape, ResultCollector<MiFrameKey> matcher)
  {
    
    ArrayList<Integer> overlapCells = null;
    Rectangle rectShape = null;
    
    overlapCells = new ArrayList<Integer>();
    rectShape = shape.getMBR();    
    for( int i = 0; i < cellGrid.length ; i++ )
    {
      if ( rectShape.x2 > cellGrid[i].x1 && cellGrid[i].x2 > rectShape.x1 
            && rectShape.y2 > cellGrid[i].y1 && cellGrid[i].y2 > rectShape.y1 )
      {
        overlapCells.add( i );
      }
    }
    
    for( Integer cellId : overlapCells )
    {     
      matcher.collect( new MiFrameKey( indexToFrameMap[cellId], cellId ) );
    }

  }
  
  public CellInfo getPartition( int indexId )
  {
    return cellGrid[indexId].clone();
  }
  
  public int getPartitionCount()
  {
    return this.numPartitions;
  }
  
  public int getMinDistanceCell( CellInfo origin, CellInfo[] cells )
  {
    double minDistance = Double.MAX_VALUE;
    double dist = -1;
    int minCell = -1;
    
    for( int i = 0; i < cells.length; i++ )
    {
      if ( cells[i] != null )
      {
        dist = getMinDistance( origin, cells[i] );
        if ( dist < minDistance )
        {
          minDistance = dist;
          minCell = i;
        }
      }
    }
    
    return minCell;
  }
  
  public double getMinDistance( CellInfo origin, CellInfo target )
  {
    double minDistance = Double.MAX_VALUE;
    double dist = 0;

    minDistance = ( minDistance < ( dist = origin.getMinDistanceTo( target.x1, target.y1)) )? minDistance : dist;
    minDistance = ( minDistance < ( dist = origin.getMinDistanceTo( target.x1, target.y2)) )? minDistance : dist;
    minDistance = ( minDistance < ( dist = origin.getMinDistanceTo( target.x2, target.y1)) )? minDistance : dist;
    minDistance = ( minDistance < ( dist = origin.getMinDistanceTo( target.x2, target.y2)) )? minDistance : dist;
    
    return minDistance;
  }
  
  public int getMaxDistanceIdx( double[] distances )
  {
    int maxDistanceIdx = 0;
    double maxValue = 0; 
    
    assert( distances.length > 0 );
    
    maxValue = distances[0];
    for(int i=1;i < distances.length;i++)
    { 
      if(distances[i] > maxValue)
      { 
         maxValue = distances[i];
         maxDistanceIdx = i;
      } 
    } 
    
    return maxDistanceIdx; 
  }
  

  @Override
  public void write(DataOutput out) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // TODO Auto-generated method stub
    
  }  
  
  public static void setLocator(Configuration conf, MiFrameLocator partitioner) throws IOException {
    conf.setClass(LOCATORCLASS, partitioner.getClass(), MiFrameLocator.class);
    Path tempFile;
    FileSystem fs = FileSystem.get(conf);
    do {
      tempFile = new Path("cells_"+(int)(Math.random()*1000000)+".partitions");
    } while (fs.exists(tempFile));
    FSDataOutputStream out = fs.create(tempFile);
    partitioner.write(out);
    out.close();
    
    fs.deleteOnExit(tempFile);

    DistributedCache.addCacheFile(tempFile.toUri(), conf);
    conf.set(LOCATORVALUE, tempFile.getName());
  }
  
  /**
   * Retrieves the value of a partitioner for a given job.
   * @param conf
   * @return
   */
  public static MiFrameLocator getLocator(Configuration conf) {
    Class<? extends MiFrameLocator> klass = conf.getClass(LOCATORCLASS, MiFrameLocator.class).asSubclass(MiFrameLocator.class);
    if (klass == null)
      return null;
    try {
      MiFrameLocator partitioner = klass.newInstance();

      String partitionerFile = conf.get(LOCATORVALUE);
      if (partitionerFile != null) {
        Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
        for (Path cacheFile : cacheFiles) {
          if (cacheFile.getName().contains(partitionerFile)) {
            FSDataInputStream in = FileSystem.getLocal(conf).open(cacheFile);
            partitioner.readFields(in);
            in.close();
          }
        }
      }
      return partitioner;
    } catch (InstantiationException e) {
      Log.warn("Error instantiating partitioner", e);
      return null;
    } catch (IllegalAccessException e) {
      Log.warn("Error instantiating partitioner", e);
      return null;
    } catch (IOException e) {
      Log.warn("Error retrieving partitioner value", e);
      return null;
    }
  }
}
