package gr.tuc.softnet.mapred.examples.countmin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

import gr.tuc.softnet.mapred.MCReducer;

/**
 * Created by ap0n on 28/4/2016.
 */
public class CountMinLocalReducer extends MCReducer<Sketch, IntWritable, IntWritable, Sketch> {

  @Override
  protected void reduce(Sketch key, Iterable<IntWritable> values,
                        Reducer<Sketch, IntWritable, IntWritable, Sketch>.Context context)
      throws IOException, InterruptedException {
    int sum = 0;
    Iterator<IntWritable> iter = values.iterator();
    while (iter.hasNext()) {
      sum += iter.next().get();
    }
    key.setSum(sum);
    context.write(new IntWritable(key.getRowIndex()), key);
  }

  @Override
  protected void cleanup(Reducer<Sketch, IntWritable, IntWritable, Sketch>.Context context)
      throws IOException, InterruptedException {
      System.out.println(getClass().getName() + " finished!");
  }
}
