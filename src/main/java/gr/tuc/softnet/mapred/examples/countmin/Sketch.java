package gr.tuc.softnet.mapred.examples.countmin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ap0n on 28/4/2016.
 */
// TODO: Serialize & test me
public class Sketch implements WritableComparable<Sketch> {

  private int rowIndex;
  private int colIndex;
  private int sum;
  private int[] row;

  public int[] getRow() {
    return row;
  }

  public void setRow(int[] row) {
    this.row = row;
  }

  public int getSum() {
    return sum;
  }

  public void setSum(int sum) {
    this.sum = sum;
  }

  public int getRowIndex() {
    return rowIndex;
  }

  public void setRowIndex(int rowIndex) {
    this.rowIndex = rowIndex;
  }

  public int getColIndex() {
    return colIndex;
  }

  public void setColIndex(int colIndex) {
    this.colIndex = colIndex;
  }

  @Override
  public void write(DataOutput out) throws IOException {

  }

  @Override
  public void readFields(DataInput in) throws IOException {

  }

  @Override
  public int compareTo(Sketch o) {
    return 0;
  }
}
