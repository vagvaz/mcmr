package gr.tuc.softnet.mapred.examples.kmeans;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

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

    for (int i = 0; i < k; i++) {
      norms[i] = con.getConfiguration().getDouble("norm" + String.valueOf(i), .0);
      // TODO(ap0n): How do we get the centroids (Document[]) from the config?
//      Map<Text, DoubleWritable> map = new HashMap<>();
//      Text centroid = new Text(con.getConfiguration().get("centroid" + String.valueOf(i)));
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
