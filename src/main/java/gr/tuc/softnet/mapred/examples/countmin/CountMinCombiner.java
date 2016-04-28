package gr.tuc.softnet.mapred.examples.countmin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

import gr.tuc.softnet.mapred.MCReducer;

/**
 * Created by ap0n on 28/4/2016.
 */
public class CountMinCombiner extends MCReducer<IntWritable, Sketch, IntWritable, Sketch> {

  @Override
  protected void reduce(IntWritable key, Iterable<Sketch> values,
                        Reducer<IntWritable, Sketch, IntWritable, Sketch>.Context context)
      throws IOException, InterruptedException {
    int sum = 0;
    Iterator<Sketch> iter = values.iterator();
    while (iter.hasNext()) {
      Sketch s = iter.next();
      sum += s.getSum();
    }

    Sketch s = new Sketch();
    s.setSum(sum);
    context.write(key, s);
  }

  @Override
  protected void cleanup(Reducer<IntWritable, Sketch, IntWritable, Sketch>.Context context)
      throws IOException, InterruptedException {
    System.out.println(getClass().getName() + " finished!");
  }
}
