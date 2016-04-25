package gr.tuc.softnet.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by vagvaz on 01/04/16.
 */
public class WordCountMapper extends MCMapper<Text, Text, Text, IntWritable> {

  @Override
  protected void map(Text key, Text value, Mapper<Text, Text, Text, IntWritable>.Context context)
      throws IOException, InterruptedException {

    System.out.println("key " + key.toString() + " value " + value.toString());
    for (String word : value.toString().split(" ")) {
      if (word != null && word.length() > 0) {
        context.write(new Text(word), new IntWritable(1));
      }
    }
  }
}
