package gr.tuc.softnet.mapred.examples.kmeans;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import gr.tuc.softnet.mapred.MCReducer;

/**
 * Created by ap0n on 28/4/2016.
 */
public class KMeansCombiner extends MCReducer<IntWritable, Document, IntWritable, Document> {

  @Override
  protected void reduce(IntWritable key, Iterable<Document> values,
                        Reducer<IntWritable, Document, IntWritable, Document>.Context context)
      throws IOException, InterruptedException {
    int documentsCount = 0;
    Map<String, Double> dimensions = new HashMap<>();
    String clusterDocuments = "";

    Iterator<Document> iter = values.iterator();
    while (iter.hasNext()) {
      Document document = iter.next();
      for (Map.Entry<String, Double> e : document.getDimensions().entrySet()) {
        if (e.getKey().equals("~")) {
          clusterDocuments += String.valueOf(e.getValue() + " ");
          continue;  // Skip document if (when used as combiner)
        }
        Double wordFrequency = e.getValue();
        Double currentFrequency = dimensions.get(e.getKey());
        if (currentFrequency == null) {
          dimensions.put(e.getKey(), wordFrequency);
        } else {
          dimensions.put(e.getKey(), currentFrequency + wordFrequency);
        }
      }
      documentsCount += document.getDocumentsCount();

      if (document.getClusterDocuments() != null) {
        // Carry the clusterDocuments (when user as localReducer)
        clusterDocuments += document.getClusterDocuments() + " ";
      }
    }

    Document toEmit = new Document(dimensions, documentsCount, clusterDocuments);
    context.write(key, toEmit);
  }
}
