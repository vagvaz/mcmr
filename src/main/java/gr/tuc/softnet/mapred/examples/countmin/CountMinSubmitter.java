package gr.tuc.softnet.mapred.examples.countmin;

import com.google.inject.Injector;

import org.apache.hadoop.io.IntWritable;

import java.io.File;
import java.util.ArrayList;
import java.util.Vector;

import gr.tuc.softnet.core.JobSubmitter;
import gr.tuc.softnet.core.NodeManager;
import gr.tuc.softnet.engine.JobConfiguration;
import gr.tuc.softnet.engine.MCJobProxy;
import gr.tuc.softnet.mapred.examples.LineDataLoader;

/**
 * Created by ap0n on 23/5/2016.
 */
public class CountMinSubmitter {

  /**
   * Submits the job but assumes the data is already loaded
   * @param isMapPipeline
   * @param isLocalReducePipeline
   * @param isFederationReducePipeline
   * @param input
   * @param output
   * @param clouds
   * @return
   */
  static public MCJobProxy submit(boolean isMapPipeline,
                                  boolean isLocalReducePipeline,
                                  boolean isFederationReducePipeline,
                                  String input,
                                  String output,
                                  ArrayList<String> clouds,
                                  int w,
                                  int d) {
    JobConfiguration c = new JobConfiguration();

    c.setMapperClass(CountMinMapper.class);
    c.setMapOutputKeyClass(Sketch.class);
    c.setMapOutputValueClass(IntWritable.class);
    c.setIsMapPipeline(isMapPipeline);

    c.setCombinerClass(CountMinCombiner.class);

    c.setLocalReduceOutputKeyClass(IntWritable.class);
    c.setLocalReduceOutputValueClass(Sketch.class);
    c.setIsLocalReducePipeline(isLocalReducePipeline);

    c.setFederationReduceOutputKeyClass(IntWritable.class);
    c.setFederationReduceOutputValueClass(Sketch.class);
    c.setIsFederationReducePipeline(isFederationReducePipeline);

    c.setClouds(clouds);

    c.setInput(input);
    c.setOutput(output);

    c.setJobProperty("w", w);
    c.setJobProperty("d", d);

    return JobSubmitter.submitJob(c);
  }

  /**
   * Loads the data and then submits the job.
   * @param isMapPipeline
   * @param isLocalReducePipeline
   * @param isFederationReducePipeline
   * @param input
   * @param output
   * @param clouds
   * @param inputFiles
   * @param threadPoolSize
   * @param nodeManager
   * @param injector
   * @return
   */
  static public MCJobProxy submit(boolean isMapPipeline,
                                  boolean isLocalReducePipeline,
                                  boolean isFederationReducePipeline,
                                  String input,
                                  String output,
                                  ArrayList<String> clouds,
                                  String inputFiles,
                                  int threadPoolSize,
                                  NodeManager nodeManager,
                                  Injector injector,
                                  int w,
                                  int d) {

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
      threads.add(new Thread(new LineDataLoader(input, nodeManager, injector, files)));
    }
    System.out.println("Loading lines to '" + input + "' KVS\n");

    threads.forEach(Thread::start);

    for (Thread t : threads) {
      try {
        t.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    return submit(isMapPipeline, isLocalReducePipeline, isFederationReducePipeline, input, output,
                  clouds, w, d);
  }
}
