package gr.tuc.softnet.mapred.examples.kmeans;

import com.google.inject.Injector;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.util.ArrayList;
import java.util.Map;
import java.util.Vector;

import gr.tuc.softnet.core.JobSubmitter;
import gr.tuc.softnet.core.NodeManager;
import gr.tuc.softnet.engine.JobConfiguration;
import gr.tuc.softnet.engine.MCJobProxy;
import gr.tuc.softnet.mapred.examples.DataLoader;
import gr.tuc.softnet.mapred.examples.wordcount.FrequencyDataLoader;

/**
 * Created by ap0n on 23/5/2016.
 */
public class KMeansSubmitter {

  private NodeManager nodeManager;
  private Injector injector;

  public KMeansSubmitter(NodeManager nodeManager, Injector injector) {
    this.nodeManager = nodeManager;
    this.injector = injector;
  }

  /**
   * Submits the job but assumes the data is already loaded
   */
  public MCJobProxy submit(boolean isMapPipeline,
                           boolean isLocalReducePipeline,
                           boolean isFederationReducePipeline,
                           String input,
                           String output,
                           ArrayList<String> clouds,
                           int k,
                           String centroidsKvsNamePrefix,
                           Double[] norms) {
    JobConfiguration c = new JobConfiguration();

    c.setMapperClass(KMeansMapper.class);
    c.setMapOutputKeyClass(IntWritable.class);
    c.setMapOutputValueClass(Document.class);
    c.setIsMapPipeline(isMapPipeline);

    c.setCombinerClass(KMeansCombiner.class);

    c.setLocalReduceOutputKeyClass(IntWritable.class);
    c.setLocalReduceOutputValueClass(Document.class);
    c.setIsLocalReducePipeline(isLocalReducePipeline);

    c.setFederationReduceOutputKeyClass(IntWritable.class);
    c.setFederationReduceOutputValueClass(Document.class);
    c.setIsFederationReducePipeline(isFederationReducePipeline);

    c.setClouds(clouds);

    c.setInput(input);
    c.setOutput(output);

    c.setJobProperty("k", k);
    c.setJobProperty("centroidsKvsNamePrefix",
                     centroidsKvsNamePrefix);  // the KVS where the centroids are
    // stored
    if (norms.length != k) {
      throw new RuntimeException("Norms count != k!");
    }
    for (int i = 0; i < norms.length; i++) {
      c.setJobProperty("norm" + String.valueOf(i), norms[i]);
    }

    return JobSubmitter.submitJob(c);
  }

  /**
   * Loads the data and then submits the job.
   */
  public MCJobProxy submit(boolean isMapPipeline,
                           boolean isLocalReducePipeline,
                           boolean isFederationReducePipeline,
                           String input,
                           String output,
                           ArrayList<String> clouds,
                           String inputFiles,
                           int threadPoolSize,
                           int k) {

    File datasetDirectory = new File(inputFiles);
    File[] allFiles = datasetDirectory.listFiles();
    Vector<File> files = new Vector<>();
    Vector<File> centroids = new Vector<>(k);
    String centroidKvsNamePrefix = "centroidsKvs";

    for (int i = 0; i < allFiles.length; i++) {
      // Load all files to a vector
      files.add(allFiles[i]);
      if (i < k) {
        centroids.add(allFiles[i]);
      }
    }

    // Load Centroids and calculate their norms
    Double[] norms = new Double[k];
    for (int i = 0; i < k; i++) {
      System.out.println(
          "Loading centroid " + String.valueOf(i) + " to '" + centroidKvsNamePrefix
                                                  + String.valueOf(i) + "' KVS\n");
      DataLoader centroidDataLoader = new FrequencyDataLoader(
          centroidKvsNamePrefix + String.valueOf(i), nodeManager, injector, null);
      try {
        Map<Text, DoubleWritable> centroid = centroidDataLoader.parseData(files.get(i));
        centroidDataLoader.load(centroid);
        double norm = 0.0;
        for (Map.Entry<Text, DoubleWritable> e : centroid.entrySet()) {
          if (e.getKey().equals("~")) {
            continue;
          }
          norm += e.getValue().get() * e.getValue().get();
        }
        norms[i] = norm;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    System.out.println("Loading data to '" + input + "' KVS\n");
    loadFiles(new FrequencyDataLoader(input, nodeManager, injector, files), threadPoolSize);

    return submit(isMapPipeline, isLocalReducePipeline, isFederationReducePipeline, input, output,
                  clouds, k, centroidKvsNamePrefix, norms);
  }

  private void loadFiles(DataLoader dataLoader, int threadPoolSize) {
    Vector<Thread> threads = new Vector<>(threadPoolSize);

    for (int i = 0; i < threadPoolSize; i++) {
      // Create the threads and pass 'files' to all of them
      threads.add(new Thread(dataLoader));
    }

    threads.forEach(Thread::start);

    for (Thread t : threads) {
      try {
        t.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
