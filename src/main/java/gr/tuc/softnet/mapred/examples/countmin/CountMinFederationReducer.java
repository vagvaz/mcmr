package gr.tuc.softnet.mapred.examples.countmin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import gr.tuc.softnet.kvs.KVSConfiguration;
import gr.tuc.softnet.kvs.MapDBSingleKVS;
import gr.tuc.softnet.mapred.MCReducer;

/**
 * Created by ap0n on 28/4/2016.
 */
public class CountMinFederationReducer extends MCReducer<IntWritable, Sketch, IntWritable, Sketch> {

  private int w;
  private MapDBSingleKVS<IntWritable, Sketch> storage;
  private boolean isLocal;
  private boolean isPipelined;

  @Override
  protected void setup(Reducer<IntWritable, Sketch, IntWritable, Sketch>.Context context)
      throws IOException, InterruptedException {
    Context con = (MCReducer.Context) context;
    this.w = con.getConfiguration().getInt("w", 1);
    this.isLocal = con.isLocalReduce();
    this.isPipelined = con.isPipelined();
    if (isPipelined) {
      KVSConfiguration rowConfig = new KVSConfiguration();
      rowConfig.setKeyClass(IntWritable.class);
      rowConfig.setValueClass(Sketch.class);
      rowConfig.setName(
          "countmin-federation-reducer-pipeline-row");  // TODO(ap0n): Should get this from somewhere.
      storage = new MapDBSingleKVS<>(rowConfig);
    }
  }

  @Override
  protected void reduce(IntWritable key, Iterable<Sketch> values,
                        Reducer<IntWritable, Sketch, IntWritable, Sketch>.Context context)
      throws IOException, InterruptedException {
    int[] singleRow = new int[w];
    int element = 0;

    Iterator<Sketch> iter = values.iterator();
    while (iter.hasNext()) {
      Sketch s = iter.next();
      if (isLocal) {
        int sum = s.getSum();
        int column = s.getColIndex();
        singleRow[column] += sum;
      } else {
        element += s.getSum();
      }
    }

    if (isPipelined) {
      if (isLocal) {
        Sketch s = storage.get(key);
        int[] row = s.getRow();
        if (row == null) {
          row = new int[w];
          for (int i = 0; i < w; i++) {
            row[i] = singleRow[i];
          }
        } else {
          row = s.getRow();
          for (int i = 0; i < w; i++) {
            row[i] = row[i] + singleRow[i];
          }
        }
        s.setRow(row);
        try {
          storage.put(key, s);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        Sketch s = storage.get(key);
        if (s == null) {
          s = new Sketch();
          s.setSum(element);
        } else {
          s.setSum(s.getSum() + element);
        }
        try {
          storage.put(key, s);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    } else {
      Sketch s = new Sketch();
      if (isLocal) {
        s.setRow(singleRow);
      } else {
        s.setSum(element);
      }
      context.write(key, s);
    }
  }

  @Override
  protected void cleanup(Reducer<IntWritable, Sketch, IntWritable, Sketch>.Context context)
      throws IOException, InterruptedException {
    System.out.println(getClass().getName() + " finished!");

    if (isPipelined) {
      Iterator<Map.Entry<IntWritable, Sketch>> iter = storage.iterator().iterator();
      while (iter.hasNext()) {
        Map.Entry<IntWritable, Sketch> e = iter.next();
        context.write(e.getKey(), e.getValue());
      }
    }
  }
}
