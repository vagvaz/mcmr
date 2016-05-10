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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import gr.tuc.softnet.mapred.examples.countmin.Document;

/**
 * Created by ap0n on 10/5/2016.
 */
public class TestCountMinDocument extends TestCase {

  @Test
  public void testWritable() throws IOException {
    Document originalDocument = generateDocument();
    String filename = "testCountMinWritable.bin";
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
    String filename = "testcountMinSerialization.bin";
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
    Document doc = new Document();
    List<String> words = new ArrayList<>(1000);
    for (int i = 0; i < 1000; i++) {
      words.add(UUID.randomUUID().toString());
    }
    doc.setWords(words);
    return doc;
  }

  private void validateDocument(Document original, Document recovered) {
    assertNotNull(recovered);
    assertNotNull(recovered.getWords());
    assertEquals(original.getWords().size(), recovered.getWords().size());
    for (int i = 0; i < original.getWords().size(); i++) {
      assertEquals(original.getWords().get(i), recovered.getWords().get(i));
    }
  }
}
