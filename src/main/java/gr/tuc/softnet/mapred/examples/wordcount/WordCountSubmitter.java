package gr.tuc.softnet.mapred.examples.wordcount;

import com.google.inject.Injector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import gr.tuc.softnet.core.JobSubmitter;
import gr.tuc.softnet.core.NodeManager;
import gr.tuc.softnet.engine.JobConfiguration;
import gr.tuc.softnet.mapred.examples.DataLoader;

/**
 * Created by ap0n on 23/5/2016.
 */
public class WordCountSubmitter {

  static public void submit(boolean isMapPipeline,
                            boolean isLocalReducePipeline,
                            boolean isFederationReducePipeline,
                            String input,
                            String output,
                            ArrayList<String> clouds) {

    JobConfiguration c = new JobConfiguration();

    c.setMapperClass(WordCountMapper.class);
    c.setMapOutputKeyClass(Text.class);
    c.setMapOutputValueClass(IntWritable.class);
    c.setIsMapPipeline(isMapPipeline);

    c.setCombinerClass(WordCountReducer.class);

    c.setLocalReduceOutputKeyClass(Text.class);
    c.setLocalReduceOutputValueClass(IntWritable.class);
    c.setIsLocalReducePipeline(isLocalReducePipeline);

    c.setFederationReduceOutputKeyClass(Text.class);
    c.setFederationReduceOutputValueClass(Text.class);
    c.setIsFederationReducePipeline(isFederationReducePipeline);

    c.setClouds(clouds);

    c.setInput(input);
    c.setOutput(output);

    JobSubmitter.submitJob(c);
  }

  static public void submit(boolean isMapPipeline,
                            boolean isLocalReducePipeline,
                            boolean isFederationReducePipeline,
                            String input,
                            String output,
                            ArrayList<String> clouds,
                            String inputFiles,
                            int threadPoolSize,
                            NodeManager nodeManager,
                            Injector injector) {

    File datasetDirectory = new File(inputFiles);
    File[] allFiles = datasetDirectory.listFiles();
    Vector<File> files = new Vector<>();

    for (File f : allFiles) {
      // Load all files to a vector
      files.add(f);
    }

    Vector<Thread> threads = new Vector<>(threadPoolSize);

    for (int i = 0; i < threadPoolSize; i++) {
      // Create the threads and pass 'files' to all of them
      threads.add(new Thread(new DataPutter(files, input, nodeManager, injector)));
    }
    System.out.println("Loading data to '" + input + "' KVS\n");

    for (Thread t : threads) {
      t.start();
    }

    for (Thread t : threads) {
      try {
        t.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    submit(isMapPipeline, isLocalReducePipeline, isFederationReducePipeline, input, output, clouds);
  }

  private static class DataPutter implements Runnable {

    Vector<File> filesToLoad;
    String kvsName;
    NodeManager nodeManager;
    Injector injector;

    public DataPutter(Vector<File> filesToLoad, String kvsName,
                      NodeManager nodeManager, Injector injector) {
      this.filesToLoad = filesToLoad;
      this.kvsName = kvsName;
      this.nodeManager = nodeManager;
      this.injector = injector;
    }

    @Override
    public void run() {
      File f;
      for (; ; ) {
        Map<IntWritable, Text> data = new HashMap<>();
        try {
          // Read each file and load its data
          f = filesToLoad.remove(0);
          BufferedReader bufferedReader
              = new BufferedReader(new InputStreamReader(new FileInputStream(f)));

          int lineCount = 0;
          String line;
          while ((line = bufferedReader.readLine()) != null) {
            data.put(new IntWritable(lineCount++), new Text(line));
          }

          DataLoader dl = new DataLoader(kvsName, nodeManager, injector, Text.class, Text.class);
          dl.load(data);

          bufferedReader.close();
        } catch (Exception e) {
          if (e instanceof ArrayIndexOutOfBoundsException) {
            break;
          }
          e.printStackTrace();
        }
      }
    }
  }
}
