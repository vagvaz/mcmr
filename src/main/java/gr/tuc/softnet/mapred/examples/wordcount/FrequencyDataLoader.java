package gr.tuc.softnet.mapred.examples.wordcount;

import com.google.inject.Injector;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import gr.tuc.softnet.core.NodeManager;
import gr.tuc.softnet.mapred.examples.DataLoader;

/**
 * Loads words frequencies. key = word, value = it's count. Also puts the document's id at the "~"
 * key (id = file.hashCode).
 *
 * Created by ap0n on 23/5/2016.
 */
public class FrequencyDataLoader extends DataLoader<Text, DoubleWritable> {

  public FrequencyDataLoader(String kvsName,
                             NodeManager nodeManager, Injector injector, Vector<File> filesToLoad) {
    super(kvsName, nodeManager, injector, Text.class, DoubleWritable.class);
    this.filesToLoad = filesToLoad;
  }

  @Override
  public Map<Text, DoubleWritable> parseData(File f) throws IOException {
    Map<Text, DoubleWritable> frequencies = new HashMap<>();
    BufferedReader bufferedReader
        = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
    String line;
    while ((line = bufferedReader.readLine()) != null) {
      String[] words = line.split(" ");

      for (String word : words) {
        if (word.length() == 0) {
          continue;
        }
        DoubleWritable wordFrequency = frequencies.get(word);
        if (wordFrequency == null) {
          frequencies.put(new Text(word), new DoubleWritable(1d));
        } else {
          frequencies.put(new Text(word), new DoubleWritable(1d + wordFrequency.get()));
        }
      }
    }

    frequencies.put(new Text("~"), new DoubleWritable((double) f.hashCode()));
    bufferedReader.close();
    return frequencies;
  }
}
