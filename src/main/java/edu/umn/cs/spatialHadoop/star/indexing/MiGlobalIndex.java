/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.star.indexing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.QuickSort;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.GridRecordWriter;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.ResultCollector2;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialAlgorithms;
import edu.umn.cs.spatialHadoop.indexing.GlobalIndex;
import edu.umn.cs.spatialHadoop.indexing.Partition;
import edu.umn.cs.spatialHadoop.mapred.ShapeIterRecordReader;
import edu.umn.cs.spatialHadoop.mapred.SpatialRecordReader.ShapeIterator;

/**
 * A very simple index frame and spatial micro-index that provides some spatial operations based
 * on an array storage.
 * @author Ahmed Eldawy
 * @@author Kwang Woo Nam
 * @param <S>
 */
public class MiGlobalIndex<T, S extends Shape> implements Writable, Iterable<S> {
  
  /**A stock instance of S used to deserialize objects from disk*/
  protected S stockShape;
  
  protected T[] indexFrames;
  
  /**All underlying shapes in no specific order*/
  protected S[] microIndexes;

  /**Whether partitions in this global index are compact (minimal) or not*/
  private boolean compact;
  
  /**Whether objects are allowed to replicated in different partitions or not*/
  private boolean replicated;
  
  public MiGlobalIndex() {
  }
  
  @SuppressWarnings("unchecked")
  public void bulkLoad(S[] shapes) {
    // Create a shallow copy
    this.microIndexes = shapes.clone();
    // Change it into a deep copy by cloning each instance
    for (int i = 0; i < this.microIndexes.length; i++) {
      this.microIndexes[i] = (S) this.microIndexes[i].clone();
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(microIndexes.length);
    for (int i = 0; i < microIndexes.length; i++) {
      microIndexes[i].write(out);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    this.microIndexes = (S[]) new Shape[length];
    for (int i = 0; i < length; i++) {
      this.microIndexes[i] = (S) stockShape.clone();
      this.microIndexes[i].readFields(in);
    }
  }
  
  public int rangeQuery(Shape queryRange, ResultCollector<S> output) {
    int result_count = 0;
    for (S shape : microIndexes) {
      if (shape.isIntersected(queryRange)) {
        result_count++;
        if (output != null) {
          output.collect(shape);
        }
      }
    }
    return result_count;
  }
  
  public static<S1 extends Shape, S2 extends Shape>
      int spatialJoin(MiGlobalIndex<S1> s1, MiGlobalIndex<S2> s2,
          final ResultCollector2<S1, S2> output) {
    return SpatialAlgorithms.SpatialJoin_planeSweep(s1.microIndexes, s2.microIndexes, output, null);
  }
  
  /**
   * A simple iterator over all shapes in this index
   * @author eldawy
   *
   */
  class SimpleIterator implements Iterator<S> {
    
    /**Current index*/
    int i = 0;

    @Override
    public boolean hasNext() {
      return i < microIndexes.length;
    }

    @Override
    public S next() {
      return microIndexes[i++];
    }

    @Override
    public void remove() {
      throw new RuntimeException("Not implemented");
    }
    
  }

  @Override
  public Iterator<S> iterator() {
    return new SimpleIterator();
  }
  
  /**
   * Number of objects stored in the index
   * @return
   */
  public int size() {
    return microIndexes.length;
  }

  /**
   * Returns the minimal bounding rectangle of all objects in the index.
   * If the index is empty, <code>null</code> is returned.
   * @return - The MBR of all objects or <code>null</code> if empty
   */
  public Rectangle getMBR() {
    Iterator<S> i = this.iterator();
    if (!i.hasNext())
      return null;
    Rectangle globalMBR = new Rectangle(Double.MAX_VALUE, Double.MAX_VALUE,
        -Double.MAX_VALUE, -Double.MAX_VALUE);
    while (i.hasNext()) {
      globalMBR.expand(i.next().getMBR());
    }
    return globalMBR;
  }

  public int knn(final double qx, final double qy, int k, ResultCollector2<S, Double> output) {
    double query_area = (getMBR().getWidth() * getMBR().getHeight()) * k / size();
    double query_radius = Math.sqrt(query_area / Math.PI);

    boolean result_correct;
    final Vector<Double> distances = new Vector<Double>();
    final Vector<S> shapes = new Vector<S>();
    // Find results in the range and increase this range if needed to ensure
    // correctness of the answer
    do {
      // Initialize result and query range
      distances.clear(); shapes.clear();
      Rectangle queryRange = new Rectangle();
      queryRange.x1 = qx - query_radius / 2;
      queryRange.y1 = qy - query_radius / 2;
      queryRange.x2 = qx + query_radius / 2;
      queryRange.y2 = qy + query_radius / 2;
      // Retrieve all results in range
      rangeQuery(queryRange, new ResultCollector<S>() {
        @Override
        public void collect(S shape) {
          distances.add(shape.distanceTo(qx, qy));
          shapes.add((S) shape.clone());
        }
      });
      if (shapes.size() <= k) {
        // Didn't find k elements in range, double the range to get more items
        if (shapes.size() == size() || shapes.size() == k) {
          // Already returned all possible elements
          result_correct = true;
        } else {
          query_radius *= 2;
          result_correct = false;
        }
      } else {
        // Sort items by distance to get the kth neighbor
        IndexedSortable s = new IndexedSortable() {
          @Override
          public void swap(int i, int j) {
            double temp_distance = distances.elementAt(i);
            distances.set(i, distances.elementAt(j));
            distances.set(j, temp_distance);
            
            S temp_shape = shapes.elementAt(i);
            shapes.set(i, shapes.elementAt(j));
            shapes.set(j, temp_shape);
          }
          @Override
          public int compare(int i, int j) {
            // Note. Equality is not important to check because items with the
            // same distance can be ordered anyway. 
        	if (distances.elementAt(i) == distances.elementAt(j))
        		return 0;
            if (distances.elementAt(i) < distances.elementAt(j))
              return -1;
            return 1;
          }
        };
        IndexedSorter sorter = new QuickSort();
        sorter.sort(s, 0, shapes.size());
        if (distances.elementAt(k - 1) > query_radius) {
          result_correct = false;
          query_radius = distances.elementAt(k);
        } else {
          result_correct = true;
        }
      }
    } while (!result_correct);
    
    int result_size = Math.min(k,  shapes.size());
    if (output != null) {
      for (int i = 0; i < result_size; i++) {
        output.collect(shapes.elementAt(i), distances.elementAt(i));
      }
    }
    return result_size;
  }
  
  /**
   * Returns true if the partitions are compact (minimal) around its contents
   * @return
   */
  public boolean isCompact() {
    return this.compact;
  }

  public void setCompact(boolean compact) {
    this.compact = compact;
  }

  public void setReplicated(boolean r) {
    this.replicated = r;
  }
  
  public boolean isReplicated() {
    return replicated;
  }
  
  /**
   * Returns the global index (partitions) of a file that is indexed using
   * the index command. If the file is not indexed, it returns null.
   * The return value is of type {@link GlobalIndex} where the generic
   * parameter is specified as {@link Partition}.
   * @param fs
   * @param dir
   * @return
   */
  public static MiGlobalIndex<MiFrame, MicroIndex> getGlobalIndex(FileSystem fs, Path dir) {
    try {
      FileStatus[] allFiles;
      if (OperationsParams.isWildcard(dir)) {
        allFiles = fs.globStatus(dir);
      } else {
        allFiles = fs.listStatus(dir);
      }
      
      FileStatus masterFile = null;
      int nasaFiles = 0;
      for (FileStatus fileStatus : allFiles) {
        if (fileStatus.getPath().getName().startsWith("_master")) {
          if (masterFile != null)
            throw new RuntimeException("Found more than one master file in "+dir);
          masterFile = fileStatus;
        } else if (fileStatus.getPath().getName().toLowerCase().matches(".*h\\d\\dv\\d\\d.*\\.(hdf|jpg|xml)")) {
          // Handle on-the-fly global indexes imposed from file naming of NASA data
          nasaFiles++;
        }
      }
      if (masterFile != null) {
        
        ShapeIterRecordReader reader = new ShapeIterRecordReader(
            fs.open(masterFile.getPath()), 0, masterFile.getLen());
        Rectangle dummy = reader.createKey();
        reader.setShape(new Partition());        
        ShapeIterator values = reader.createValue();
        
        ArrayList<MiFrame> indexFrames = new ArrayList<MiFrame>();
        while (reader.next(dummy, values)) {
          for (Shape value : values) {
            indexFrames.add((MiFrame) value.clone());
          }
        }
        MiGlobalIndex<MiFrame, MicroIndex> globalIndex = new MiGlobalIndex<MiFrame, MicroIndex>();
        globalIndex.bulkLoad(indexFrames.toArray(new Partition[indexFrames.size()]));
        String extension = masterFile.getPath().getName();
        extension = extension.substring(extension.lastIndexOf('.') + 1);
        globalIndex.setCompact(GridRecordWriter.PackedIndexes.contains(extension));
        globalIndex.setReplicated(GridRecordWriter.ReplicatedIndexes.contains(extension));
        return globalIndex;
      } else if (nasaFiles > allFiles.length / 2) {
        // A folder that contains HDF files
        // Create a global index on the fly for these files based on their names
        Partition[] partitions = new Partition[allFiles.length];
        for (int i = 0; i < allFiles.length; i++) {
          final Pattern cellRegex = Pattern.compile(".*(h\\d\\dv\\d\\d).*");
          String filename = allFiles[i].getPath().getName();
          Matcher matcher = cellRegex.matcher(filename);
          Partition partition = new Partition();
          partition.filename = filename;
          if (matcher.matches()) {
            String cellname = matcher.group(1);
            int h = Integer.parseInt(cellname.substring(1, 3));
            int v = Integer.parseInt(cellname.substring(4, 6));
            partition.cellId = v * 36 + h;
            // Calculate coordinates on MODIS Sinusoidal grid
            partition.x1 = h * 10 - 180;
            partition.y2 = (18 - v) * 10 - 90;
            partition.x2 = partition.x1 + 10;
            partition.y1 = partition.y2 - 10;
            // Convert to Latitude Longitude
            double lon1 = partition.x1 / Math.cos(partition.y1 * Math.PI / 180);
            double lon2 = partition.x1 / Math.cos(partition.y2 * Math.PI / 180);
            partition.x1 = Math.min(lon1, lon2);
            lon1 = partition.x2 / Math.cos(partition.y1 * Math.PI / 180);
            lon2 = partition.x2 / Math.cos(partition.y2 * Math.PI / 180);
            partition.x2 = Math.max(lon1, lon2);
          } else {
            partition.set(-180, -90, 180, 90);
            partition.cellId = allFiles.length + i;
          }
          partitions[i] = partition;
        }
        GlobalIndex<Partition> gindex = new GlobalIndex<Partition>();
        gindex.bulkLoad(partitions);
        return gindex;
      } else {
        return null;
      }
    } catch (IOException e) {
      LOG.info("Error retrieving global index of '"+dir+"'");
      LOG.info(e);
      return null;
    }
  }

}
