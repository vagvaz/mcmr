package gr.tuc.softnet.mapred.examples.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import gr.tuc.softnet.mapred.MCMapper;

/**
 * Created by vagvaz on 01/04/16.
 */
public class WordCountMapper extends MCMapper<IntWritable, Text, Text, IntWritable> {

  @Override
  protected void map(IntWritable key, Text value,
                     Mapper<IntWritable, Text, Text, IntWritable>.Context context)
      throws IOException, InterruptedException {

    for (String word : value.toString().split(" ")) {
      if (word != null && word.length() > 0) {
        context.write(new Text(word), new IntWritable(1));
      }
    }
  }
}
