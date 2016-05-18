package examplestests;

import junit.framework.TestCase;

import org.junit.Test;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import gr.tuc.softnet.mapred.examples.kmeans.Document;

/**
 * Created by ap0n on 28/4/2016.
 */
public class TestKmeansDocument extends TestCase {

  @Test
  public void testWritable() throws IOException {
    Document originalDocument = generateDocument();
    String filename = "testKMeansWritable.bin";
    FileOutputStream fos = new FileOutputStream(filename);
    DataOutput out = new DataOutputStream(fos);
    originalDocument.write(out);
    fos.flush();
    fos.close();
    FileInputStream fis = new FileInputStream(filename);
    DataInput in = new DataInputStream(fis);
    Document recoveredDocument = new Document();
    recoveredDocument.readFields(in);
    fis.close();
    validateDocument(originalDocument, recoveredDocument);
  }

  @Test
  public void testSerialization() {
    Document originalDocument = generateDocument();
    String filename = "testKMeansSerialization.bin";
    FileOutputStream fos = null;
    ObjectOutputStream out = null;
    try {
      fos = new FileOutputStream(filename);
      out = new ObjectOutputStream(fos);
      out.writeObject(originalDocument);
      out.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    FileInputStream fis = null;
    ObjectInputStream in = null;
    Document recovered = null;
    try {
      fis = new FileInputStream(filename);
      in = new ObjectInputStream(fis);
      recovered = (Document) in.readObject();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    validateDocument(originalDocument, recovered);
  }

  private Document generateDocument() {
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
    Document doc = new Document(data, documentsCount, clusterDocuments);
    doc.setNorm(norm);
    doc.setIndex(index);
    return doc;
  }

  private void validateDocument(Document original, Document recovered) {
    assertEquals(original.getIndex(), recovered.getIndex());
    assertEquals(original.getNorm(), recovered.getNorm());
    assertEquals(original.getClusterDocuments(), recovered.getClusterDocuments());
    assertEquals(original.getDimensions().size(), recovered.getDimensions().size());
    assertEquals(original.getDocumentsCount(), recovered.getDocumentsCount());
    for (Map.Entry<String, Double> e : original.getDimensions().entrySet()) {
      assertNotNull(recovered.getDimensions().get(e.getKey()));
      assertEquals(original.getDimensions().get(e.getKey()),
                   recovered.getDimensions().get(e.getKey()));
    }
  }
}
