package gr.tuc.softnet.mapred.examples.countmin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by ap0n on 28/4/2016.
 *
 * TODO: rename this class. E.g. SketchHolder, SketchUtil or something like this.
 */
public class Sketch implements WritableComparable<Sketch>, Serializable {

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
    out.writeInt(rowIndex);
    out.writeInt(colIndex);
    out.writeInt(sum);
    out.writeInt(row.length);
    for (int i : row) {
      out.writeInt(i);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    rowIndex = in.readInt();
    colIndex = in.readInt();
    sum = in.readInt();
    int length = in.readInt();
    if (length > 0) {
      row = new int[length];
    }
    for (int i = 0; i < length; i++) {
      row[i] = in.readInt();
    }
  }

  @Override
  public int compareTo(Sketch o) {
    if (rowIndex == o.rowIndex) {
      if (colIndex == o.colIndex) {
        if (sum == o.sum) {
          if (row.length == o.row.length) {
            boolean eq = true;
            for (int i : row) {
              if (row[i] != o.row[i]) {
                eq = false;
              }
            }
            if (eq) {
              return 0;
            } else {
              return -1;
            }
          } else {
            return row.length - o.row.length;
          }
        } else {
          return sum - o.sum;
        }
      } else {
        return colIndex - o.colIndex;
      }
    } else {
      return rowIndex - o.rowIndex;
    }
  }
}
