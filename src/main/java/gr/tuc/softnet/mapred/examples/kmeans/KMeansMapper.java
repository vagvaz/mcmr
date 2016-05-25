package gr.tuc.softnet.mapred.examples.kmeans;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import gr.tuc.softnet.core.InjectorUtils;
import gr.tuc.softnet.kvs.KVSManager;
import gr.tuc.softnet.kvs.KVSProxy;
import gr.tuc.softnet.mapred.MCMapper;

/**
 * Created by ap0n on 25/4/2016.
 */
public class KMeansMapper extends MCMapper<Text, Document, IntWritable, Document> {

  Document[] centroids;
  Double[] norms;
  Random random;
  private int k;

  @Override
  protected void setup(Mapper<Text, Document, IntWritable, Document>.Context context)
      throws IOException, InterruptedException {
    MCMapper.Context con = (MCMapper.Context) context;
    k = con.getConfiguration().getInt("k", 1);
    centroids = new Document[k];
    norms = new Double[k];

    KVSManager kvsManager = InjectorUtils.getInjector().getInstance(KVSManager.class);

    for (int i = 0; i < k; i++) {
      // Get the norm
      norms[i] = con.getConfiguration().getDouble("norm" + String.valueOf(i), .0);

      // Load the centroid
      centroids[i] = new Document();
      centroids[i].setIndex(i);
      // Get the KVS for the centroid i
      String kvsName = con.getConfiguration().get("centroidsKvsNamePrefix") + String.valueOf(i);
      KVSProxy<Text, DoubleWritable> centroidKvs = kvsManager.getKVSProxy(kvsName);
      // Iterate over it
      Iterator<Map.Entry<Text, DoubleWritable>> iter = centroidKvs.iterator().iterator();
      while (iter.hasNext()) {
        Map.Entry<Text, DoubleWritable> e = iter.next();
        // get its dimensions
        centroids[i].putDimention(e.getKey().toString(), e.getValue().get());
      }
    }
  }

  @Override
  protected void map(Text key, Document document,
                     Mapper<Text, Document, IntWritable, Document>.Context context)
      throws IOException, InterruptedException {
    double maxSimilarity = 0;
    int index = random.nextInt(k);
    for (int i = 0; i < k; i++) {
      double d = calculateCosSimilarity(i, document);
      if (d > maxSimilarity) {
        maxSimilarity = d;
        index = i;
      }
    }
    document.setDocumentsCount(1);
    context.write(new IntWritable(index), document);
  }

  @Override
  protected void cleanup(Mapper<Text, Document, IntWritable, Document>.Context context)
      throws IOException, InterruptedException {
    System.out.println(getClass().getName() + " finished!");
  }

  private double calculateCosSimilarity(int i, Document document) {

    // cosine = A B / ||A|| ||B|| (Ignore document's norm)

    Double numerator = 0d;

    for (Map.Entry<String, Double> e : document.getDimensions().entrySet()) {

      if (e.getKey().equals("~")) {
        continue;  // Skip the document id
      }
      Double centroidValue = centroids[i].getDimensions().get(e.getKey());
      if (centroidValue != null) {
        numerator += e.getValue() * centroidValue;
      }
    }

    return numerator / Math.sqrt(Math.max(norms[i], 1));
  }
}
