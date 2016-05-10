package gr.tuc.softnet.mapred.examples.kmeans;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ap0n on 25/4/2016.
 */

/**
 * Probably not the best name for the class. It models a document but also has utility members used
 * by KMeans map-reduce app
 */
public class Document implements Writable, Serializable {

  private int index;
  private String clusterDocuments;
  private double norm;
  private Map<String, Double> dimensions;
  private int documentsCount;

  public Document() {
    index = 0;
    clusterDocuments = null;
    norm = .0;
    dimensions = new HashMap<>();
    documentsCount = 0;
  }

  public Document(Map<String, Double> dimensions, int documentsCount, String clusterDocuments) {
    this.index = 0;
    this.norm = .0;
    this.dimensions = dimensions;
    this.documentsCount = documentsCount;
    this.clusterDocuments = clusterDocuments;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(index);
    out.writeInt(documentsCount);
    out.writeDouble(norm);
    writeString(out, clusterDocuments);
    out.writeInt(dimensions.size());
    for (Map.Entry<String, Double> e : dimensions.entrySet()) {
      writeString(out, e.getKey());
      out.writeDouble(e.getValue());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    if (dimensions == null) {
      dimensions = new HashMap<>();
    } else {
      dimensions.clear();
    }
    index = in.readInt();
    documentsCount = in.readInt();
    norm = in.readDouble();
    clusterDocuments = readString(in);
    int entriesCounter = in.readInt();
    while (entriesCounter-- > 0) {
      dimensions.put(readString(in), in.readDouble());
    }
  }

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public double getNorm() {
    return norm;
  }

  public void setNorm(double norm) {
    this.norm = norm;
  }

  public Map<String, Double> getDimensions() {
    return dimensions;
  }

  public void setDimensions(Map<String, Double> dimensions) {
    this.dimensions = dimensions;
  }

  public int getDocumentsCount() {
    return documentsCount;
  }

  public void setDocumentsCount(int documentsCount) {
    this.documentsCount = documentsCount;
  }

  public String getClusterDocuments() {
    return clusterDocuments;
  }

  public void setClusterDocuments(String clusterDocuments) {
    this.clusterDocuments = clusterDocuments;
  }

  private void writeString(DataOutput out, String string) throws IOException {
    if (string == null) {
      out.writeInt(0);
    } else {
      out.writeInt(string.getBytes().length);
      out.write(string.getBytes());
    }
  }

  private String readString(DataInput in) throws IOException {
    int bytesCounter = in.readInt();
    if (bytesCounter == 0) {
      return null;
    }
    byte[] stringBytes = new byte[bytesCounter];
    for (int i = 0; i < bytesCounter; i++) {
      stringBytes[i] = in.readByte();
    }
    return new String(stringBytes);
  }
}
