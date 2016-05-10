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
import java.util.Random;

import gr.tuc.softnet.mapred.examples.countmin.Sketch;

/**
 * Created by ap0n on 10/5/2016.
 */
public class TestSketch extends TestCase {

  @Test
  public void testWritable() throws IOException {
    Sketch sketch = generateSketch();
    String filename = "testSketchWritable.bin";
    FileOutputStream fos = new FileOutputStream(filename);
    DataOutput out = new DataOutputStream(fos);
    sketch.write(out);
    fos.flush();
    fos.close();
    FileInputStream fis = new FileInputStream(filename);
    DataInput in = new DataInputStream(fis);
    Sketch recoveredSketch = new Sketch();
    recoveredSketch.readFields(in);
    fis.close();
    validateSketch(sketch, recoveredSketch);
  }

  @Test
  public void testSerialization() {
    Sketch originalDocument = generateSketch();
    String filename = "testSketchSerialization.bin";
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
    Sketch recovered = null;
    try {
      fis = new FileInputStream(filename);
      in = new ObjectInputStream(fis);
      recovered = (Sketch) in.readObject();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    validateSketch(originalDocument, recovered);
  }

  private Sketch generateSketch() {
    Random random = new Random(System.currentTimeMillis());
    Sketch sketch = new Sketch();
    sketch.setSum(random.nextInt());
    sketch.setColIndex(random.nextInt());
    sketch.setRowIndex(random.nextInt());
    int[] row = new int[random.nextInt(10)];
    for (int i = 0; i < row.length; i++) {
      row[i] = random.nextInt();
    }
    sketch.setRow(row);
    return sketch;
  }

  private void validateSketch(Sketch original, Sketch recovered) {
    assertNotNull(recovered);
    assertEquals(original.getColIndex(), recovered.getColIndex());
    assertEquals(original.getRowIndex(), recovered.getRowIndex());
    assertEquals(original.getSum(), recovered.getSum());
    assertNotNull(recovered.getRow());
    for (int i = 0; i < original.getRow().length; i++) {
      assertEquals(original.getRow()[i], recovered.getRow()[i]);
    }
  }
}
