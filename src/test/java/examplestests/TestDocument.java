package examplestests;

import junit.framework.TestCase;

import org.junit.Test;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import gr.tuc.softnet.mapred.examples.kmeans.Document;

/**
 * Created by ap0n on 28/4/2016.
 */
public class TestDocument extends TestCase {

  @Test
  public void test() throws IOException {
    Random random = new Random(System.currentTimeMillis());
    String clusterDocuments = "";
    Map<String, Double> data = new HashMap<>();
    for (int i = 0; i < 1000; i++) {
      data.put(String.valueOf(i), random.nextDouble());
      clusterDocuments += String.valueOf(i);
    }
    int documentsCount = random.nextInt(100);
    double norm = random.nextDouble();
    int index = random.nextInt();
    Document toWrite = new Document(data, documentsCount, clusterDocuments);
    toWrite.setNorm(norm);
    toWrite.setIndex(index);

    String filename = "test.bin";
    FileOutputStream fos = new FileOutputStream(filename);
    DataOutput out = new DataOutputStream(fos);
    toWrite.write(out);
    fos.flush();
    fos.close();
    FileInputStream fis = new FileInputStream(filename);
    DataInput in = new DataInputStream(fis);

    Document recovered = new Document();
    recovered.readFields(in);
    fis.close();

    assertEquals(index, recovered.getIndex());
    assertEquals(norm, recovered.getNorm());
    assertEquals(clusterDocuments, recovered.getClusterDocuments());
    assertEquals(data.size(), recovered.getDimensions().size());
    assertEquals(documentsCount, recovered.getDocumentsCount());
    for (Map.Entry<String, Double> e : data.entrySet()) {
      assertNotNull(recovered.getDimensions().get(e.getKey()));
      assertEquals(data.get(e.getKey()), recovered.getDimensions().get(e.getKey()));
    }
  }
}
