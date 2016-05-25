package gr.tuc.softnet.mapred.examples;

import com.google.inject.Injector;

import org.apache.hadoop.io.IntWritable;
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

/**
 * Loads lines of files key = lineNumber (starting from 0), value = line
 *
 * Created by ap0n on 23/5/2016.
 */
public class LineDataLoader extends DataLoader<IntWritable, Text> {

  public LineDataLoader(String kvsName,
                        NodeManager nodeManager,
                        Injector injector,
                        Vector<File> filesToLoad) {
    super(kvsName, nodeManager, injector, IntWritable.class, Text.class);
    this.filesToLoad = filesToLoad;
  }

  @Override
  public Map<IntWritable, Text> parseData(File f) throws IOException {

    // keyPrefix is not used

    Map<IntWritable, Text> data = new HashMap<>();
    BufferedReader bufferedReader
        = new BufferedReader(new InputStreamReader(new FileInputStream(f)));

    int lineCount = 0;
    String line;
    while ((line = bufferedReader.readLine()) != null) {
      data.put(new IntWritable(lineCount++), new Text(line));
    }
    bufferedReader.close();
    return data;
  }
}
