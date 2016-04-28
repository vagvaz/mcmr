package gr.tuc.softnet.mapred.examples.countmin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

import gr.tuc.softnet.mapred.MCMapper;

/**
 * Created by ap0n on 28/4/2016.
 */
public class CountMinMapper extends MCMapper<IntWritable, Document, Sketch, IntWritable> {

  private int w;
  private int d;
  private Random random;

  @Override
  protected void setup(Mapper<IntWritable, Document, Sketch, IntWritable>.Context context)
      throws IOException, InterruptedException {
    MCMapper.Context con = (MCMapper.Context) context;
    w = con.getConfiguration().getInt("w", 1);
    d = con.getConfiguration().getInt("d", 1);
    random = new Random();
  }

  @Override
  protected void map(IntWritable key, Document document,
                     Mapper<IntWritable, Document, Sketch, IntWritable>.Context context)
      throws IOException, InterruptedException {

    for (String word : document.getWords()) {
      if (word != null && word.length() > 0) {
        int[] yDim = hashRandom(word.hashCode());
        for (int i = 0; i < d; i++) {
          // emit <(<row>,<col>), count>
          Sketch outKey = new Sketch();
          outKey.setRowIndex(i);
          outKey.setColIndex(yDim[i]);
          context.write(outKey, new IntWritable(1));
        }
      }
    }
  }

  @Override
  protected void cleanup(Mapper<IntWritable, Document, Sketch, IntWritable>.Context context)
      throws IOException, InterruptedException {
    System.out.println(getClass().getName() + " finished!");
  }

  private int[] hashRandom(int seed) {
    int[] hash = new int[d];
    random.setSeed(seed);
    for (int i = 0; i < d; i++) {
      hash[i] = random.nextInt(w);
    }
    return hash;
  }
}
