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
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

public class MiFrame implements WritableComparable<MiFrame>  {
  
  protected int frameId;
  
  /**Name of the file that contains the data*/
  protected String filename;
  
  protected HashMap<Integer, MicroIndex> miIndexes = null;
  
  protected MiFrame() {}
  
  public MiFrame( int frameId, String filename, HashMap<Integer, MicroIndex> indexes ) {
    this.frameId = frameId;
    this.filename = filename;
    this.miIndexes = indexes;
  }
  
  public MiFrame(MiFrame other) {
    this.frameId = other.frameId;
    this.filename = other.filename;
    this.miIndexes = new HashMap<Integer, MicroIndex>();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt( frameId );
    out.writeUTF( filename );
    out.writeInt( miIndexes.size() );
    for( MicroIndex miInfo : miIndexes.values() )
    {
      miInfo.write( out );
    }
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    int count = 0;
    
    frameId = in.readInt();
    filename = in.readUTF();
    count = in.readInt();
    
    for( int i=0; i < count ; i++ )
    {
      MicroIndex index = new MicroIndex();
      index.readFields( in );
    }
  }
  
  public Text toText(Text text) {
    text.append(new byte[] {','}, 0, 1);
    TextSerializerHelper.serializeInt( frameId, text, ',');
    byte[] temp = (filename == null? "" : filename).getBytes();
    text.append(temp, 0, temp.length);
    TextSerializerHelper.serializeInt( miIndexes.size(), text, ',');
    for( MicroIndex indexInfo : miIndexes.values() )
    {
      indexInfo.toText(text);
    }
    
    return text;
  }
  
  public void fromText(Text text) {
    int count = 0;
    
    text.set(text.getBytes(), 1, text.getLength() - 1); // Skip comma
    this.frameId = TextSerializerHelper.consumeInt(text, ',');
    this.filename = TextSerializerHelper.consumeString(text, ',') ;
    count = TextSerializerHelper.consumeInt(text, ',');
    
    if ( count > 0 )
    {
      this.miIndexes = new HashMap<Integer,MicroIndex>(count);
      for( int i=0; i < count ; i++ )
      {
        MicroIndex indexInfo = new MicroIndex();
        
        indexInfo.fromText(text);
        this.miIndexes.put( indexInfo.getIndexId(), indexInfo);
      }
    }
  }
  
  public MicroIndex getIndex( int indexId )
  {
    return this.miIndexes.get( indexId );
  }
  
  public MicroIndex[] getIndexes()
  {
    return (MicroIndex[])this.miIndexes.values().toArray();
  }
  
  public void setFilename( String filename )
  {
    this.filename = filename;
  }
  
  public void setFrameId( int frameId )
  {
    this.frameId = frameId;
  }
  
  @Override
  public MiFrame clone() {
    return new MiFrame(this);
  }
  
  @Override
  public int hashCode() {
    return filename.hashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == null)
      return false;
    return this.filename.equals(((MiFrame)obj).filename);
  }
  
  /*
  public void draw(Graphics g, Rectangle fileMBR, int imageWidth,
      int imageHeight, double scale) {
    int s_x1 = (int) Math.round((this.x1 - fileMBR.x1) * imageWidth / fileMBR.getWidth());
    int s_y1 = (int) Math.round((this.y1 - fileMBR.y1) * imageHeight / fileMBR.getHeight());
    int s_x2 = (int) Math.round((this.x2 - fileMBR.x1) * imageWidth / fileMBR.getWidth());
    int s_y2 = (int) Math.round((this.y2 - fileMBR.y1) * imageHeight / fileMBR.getHeight());
    g.drawRect(s_x1, s_y1, s_x2 - s_x1 + 1, s_y2 - s_y1 + 1);
  }
  

  public void draw(Graphics g, double xscale, double yscale) {
    int imgx1 = (int) Math.round(this.x1 * xscale);
    int imgy1 = (int) Math.round(this.y1 * yscale);
    int imgx2 = (int) Math.round(this.x2 * xscale);
    int imgy2 = (int) Math.round(this.y2 * yscale);
    g.drawRect(imgx1, imgy1, imgx2 - imgx1 + 1, imgy2 - imgy1 + 1);
  }
  */

  public String toWKT() {
    return "MiFrame:\t"+frameId+"\t"+miIndexes.size()+"\t"+filename;
  }

  @Override
  public int compareTo(MiFrame other) {
    
    return this.frameId - other.frameId;
  }
}
