package gr.tuc.softnet.mapred.examples.kmeans;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import gr.tuc.softnet.kvs.KVSConfiguration;
import gr.tuc.softnet.kvs.MapDBSingleKVS;
import gr.tuc.softnet.mapred.MCReducer;

/**
 * Created by ap0n on 28/4/2016.
 */
public class KMeansReducer extends MCReducer<IntWritable, Document, IntWritable, Document> {

  private MapDBSingleKVS<IntWritable, Document> storage;
  private boolean isPipelined;

  @Override
  protected void setup(Reducer<IntWritable, Document, IntWritable, Document>.Context context)
      throws IOException, InterruptedException {
    Context con = (MCReducer.Context) context;
    isPipelined = con.isPipelined();
    if (isPipelined) {
      KVSConfiguration config = new KVSConfiguration();
      config.setKeyClass(IntWritable.class);
      config.setValueClass(Document.class);
      config.setName("k-means-reducer-pipeline");  // TODO(ap0n): Should get this from somewhere.
      storage = new MapDBSingleKVS<>(new KVSConfiguration());
    }
  }

  @Override
  protected void reduce(IntWritable key, Iterable<Document> values,
                        Reducer<IntWritable, Document, IntWritable, Document>.Context context)
      throws IOException, InterruptedException {
    int documentsCount = 0;
    Map<String, Double> dimensions = new HashMap<>();
    String clusterDocuments = "";  // Space-separated document ids
    Iterator<Document> iter = values.iterator();
    while (iter.hasNext()) {
      Document document = iter.next();
      documentsCount += document.getDocumentsCount();

      for (Map.Entry<String, Double> dim : document.getDimensions().entrySet()) {
        String word = dim.getKey();
        Double wordFrequency = dim.getValue();
        Double currentFrequency = dimensions.get(word);
        if (currentFrequency == null) {
          dimensions.put(word, wordFrequency);
        } else {
          dimensions.put(word, currentFrequency + wordFrequency);
        }
      }  // end of for
      clusterDocuments += document.getClusterDocuments();
    }  // end of while

    if (isPipelined) {
      Document document = storage.get(key);
      if (document != null) {
        int storedDocumentsCount = document.getDocumentsCount();
        Map<String, Double> storedDimensions = document.getDimensions();

        for (Map.Entry<String, Double> e : dimensions.entrySet()) {
          Double storedFrequency = storedDimensions.get(e.getKey());
          if (storedFrequency == null) {
            storedDimensions.put(e.getKey(), e.getValue());
          } else {
            storedDimensions.put(e.getKey(), storedFrequency + e.getValue());
          }
        }  // end of for
        document.setDimensions(storedDimensions);
        document.setDocumentsCount(documentsCount + storedDocumentsCount);
        document.setClusterDocuments(clusterDocuments + document.getClusterDocuments());
      } else {  // does not exist in storage
        document = new Document(dimensions, documentsCount, clusterDocuments);
      }
      try {
        storage.put(key, document);
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {  // not pipelined
      double norm = 0d;
      for (Map.Entry<String, Double> entry : dimensions.entrySet()) {
        entry.setValue(entry.getValue() / (double) documentsCount);
        norm += entry.getValue() * entry.getValue();
      }
      Document toEmit = new Document();
      toEmit.setDimensions(dimensions);
      toEmit.setNorm(norm);
      toEmit.setIndex(key.get());
      toEmit.setClusterDocuments(clusterDocuments);
      context.write(key, toEmit);
    }
  }

  @Override
  protected void cleanup(Reducer<IntWritable, Document, IntWritable, Document>.Context context)
      throws IOException, InterruptedException {
    System.out.println(getClass().getName() + " finished!");
    if (isPipelined) {
      Iterator<Map.Entry<IntWritable, Document>> iter = storage.iterator().iterator();
      while (iter.hasNext()) {
        Map.Entry<IntWritable, Document> e = iter.next();
        IntWritable reducedKey = e.getKey();
        Document storedDocument = e.getValue();

        double norm = 0d;
        for (Map.Entry<String, Double> entry : storedDocument.getDimensions().entrySet()) {
          entry.setValue(entry.getValue() / (double) storedDocument.getDocumentsCount());
          norm += entry.getValue() * entry.getValue();
        }
        storedDocument.setNorm(norm);
        storedDocument.setIndex(reducedKey.get());
        context.write(reducedKey, storedDocument);
      }
    }
  }
}