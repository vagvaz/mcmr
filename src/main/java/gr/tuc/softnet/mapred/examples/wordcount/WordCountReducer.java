package gr.tuc.softnet.mapred.examples.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import gr.tuc.softnet.kvs.KVSConfiguration;
import gr.tuc.softnet.kvs.MapDBSingleKVS;
import gr.tuc.softnet.mapred.MCReducer;

/**
 * Created by ap0n on 25/4/2016.
 */
public class WordCountReducer extends MCReducer<Text, IntWritable, Text, IntWritable> {

  private MapDBSingleKVS<Text, IntWritable> sums;
  private boolean storeValues;

  @Override
  protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
      throws IOException, InterruptedException {
    Context con = (MCReducer.Context) context;
    storeValues = !con.isLocalReduce() && con.isPipelined();
    if (storeValues) {
      KVSConfiguration config = new KVSConfiguration();
      config.setKeyClass(Text.class);
      config.setValueClass(IntWritable.class);
      config.setName("word-count-pipeline");  // TODO(ap0n): Should get this from somewhere.
      sums = new MapDBSingleKVS<>(new KVSConfiguration());
    }
  }

  @Override
  protected void reduce(
      Text key, Iterable<IntWritable> values,
      Reducer<Text, IntWritable, Text, IntWritable>.Context context)
      throws IOException, InterruptedException {

    int result = 0;
    Iterator<IntWritable> it = values.iterator();
    while (it.hasNext()) {
      result += it.next().get();
    }

    if (storeValues) {
      if (sums.contains(key)) {
        result += sums.get(key).get();
      }
      try {
        sums.put(key, new IntWritable(result));
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      context.write(key, new IntWritable(result));
    }
  }

  @Override
  protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
      throws IOException, InterruptedException {
    System.out.println(getClass().getName() + " finished!");
    if (storeValues) {
      Iterator<Map.Entry<Text, IntWritable>> iterator = sums.iterator().iterator();
      while (iterator.hasNext()) {
        Map.Entry<Text, IntWritable> entry = iterator.next();
        context.write(entry.getKey(), entry.getValue());
      }
    }
  }
}
