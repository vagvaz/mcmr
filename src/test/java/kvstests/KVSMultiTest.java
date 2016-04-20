package kvstests;

import junit.framework.TestCase;

import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import gr.tuc.softnet.kvs.KVSConfiguration;
import gr.tuc.softnet.kvs.LevelDBMultiKVS;
import gr.tuc.softnet.kvs.MapDBMultiKVS;

/**
 * Created by ap0n on 13/4/2016.
 */
public class KVSMultiTest extends TestCase {

  protected MapDBMultiKVS<IntWritable, IntWritable> mapdbKVS;
//  protected LevelDBMultiKVS<IntWritable, IntWritable> leveldbKVS;
  protected KVSConfiguration mapDBConfiguration;
//  protected KVSConfiguration levelDBConfiguration;

  protected String mapKVSName;
//  protected String levelKVSName;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mapKVSName = "mapKVSName";
//    levelKVSName = "levelKVSName";

    mapDBConfiguration = new KVSConfiguration();
    mapDBConfiguration.setName(mapKVSName);
    mapDBConfiguration.setBaseDir("/tmp/mapdb/");
    mapDBConfiguration.setKeyClass(IntWritable.class);
    mapDBConfiguration.setValueClass(IntWritable.class);

//    levelDBConfiguration = new KVSConfiguration();
//    levelDBConfiguration.setName(levelKVSName);
//    levelDBConfiguration.setBaseDir("/tmp/leveldb/");
//    levelDBConfiguration.setKeyClass(IntWritable.class);
//    levelDBConfiguration.setValueClass(IntWritable.class);

    mapdbKVS = new MapDBMultiKVS<>(mapDBConfiguration);
//    leveldbKVS = new LevelDBMultiKVS<>(levelDBConfiguration);
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    mapdbKVS.close();
//    leveldbKVS.close();
  }

//  @Test
//  public void testGetName() {
//    System.out.println("KVSSingeTest.testGetName");
//    assertEquals(mapKVSName, mapdbKVS.getName());
//    assertEquals(levelKVSName, leveldbKVS.getName());
//  }

//  @Test
//  public void testSize() {
//    System.out.println("KVSMultiTest.testSize");
//    Map<IntWritable, List<IntWritable>> data = new HashMap<>();
//    int keysCount = 10;
//    int valueCountPerKey = 10;
//    generateValues(keysCount, valueCountPerKey);
//
//    try {
//      appendToBothKVSs(data);
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail();
//    }
//
//    assertEquals(keysCount * valueCountPerKey, mapdbKVS.size());
//    assertEquals(keysCount * valueCountPerKey, leveldbKVS.size());
//  }
//
//  @Test
//  public void testContains() {
//    System.out.println("KVSMultiTest.testContains");
//
//    int keysCount = 100;
//    int valuesPerKeyCount = 1000;
//
//    generateValues(keysCount, valuesPerKeyCount);
//
//    for (int i = 0; i < keysCount; i++) {
//      assertTrue(mapdbKVS.contains(new IntWritable(i)));
//      assertTrue(leveldbKVS.contains(new IntWritable(i)));
//    }
//  }

  @Test
  public void testIterator() {
    System.out.println("KVSMultiTest.testIterator");

    int keysCount = 2;
    int valuesPerKeyCount = 2;

    Map<IntWritable, List<IntWritable>> data = generateValues(keysCount, valuesPerKeyCount);

    try {
      appendToBothKVSs(data);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

    verifyBothIterators(data);
  }

  private Map<IntWritable, List<IntWritable>> generateValues(int keysCount,
                                                             int valuesPerKey) {
    Map<IntWritable, List<IntWritable>> data = new HashMap<>();
    for (int i = 0; i < keysCount; i++) {
      List<IntWritable> values = new ArrayList<>(valuesPerKey);
      for (int v = 0; v < valuesPerKey; v++) {
        values.add(new IntWritable(v));
      }
      data.put(new IntWritable(i), values);
    }

    return data;
  }

  private void appendToBothKVSs(Map<IntWritable, List<IntWritable>> data) throws Exception {
    for (Map.Entry<IntWritable, List<IntWritable>> e : data.entrySet()) {
      for (IntWritable i : e.getValue()) {
        mapdbKVS.append(e.getKey(), i);
//        leveldbKVS.append(e.getKey(), i);
      }
    }
  }

  private void verifyBothIterators(Map<IntWritable, List<IntWritable>> data) {
    Iterator<Map.Entry<IntWritable, Iterator<IntWritable>>> mapDBIterator = mapdbKVS.iterator()
        .iterator();

//    Iterator<Map.Entry<IntWritable, Iterator<IntWritable>>> levelIterator = leveldbKVS.iterator()
//        .iterator();

    for (Map.Entry<IntWritable, List<IntWritable>> dataEntry : data.entrySet()) {
      assertTrue(mapDBIterator.hasNext());
//      assertTrue(levelIterator.hasNext());

      Map.Entry<IntWritable, Iterator<IntWritable>> mapDBEntry = mapDBIterator.next();
      assertNotNull(mapDBEntry);
//      Map.Entry<IntWritable, Iterator<IntWritable>> levelEntry = levelIterator.next();
//      assertNotNull(levelEntry);

      Iterator<IntWritable> mapIteratorForKey = mapDBEntry.getValue();
//      Iterator<IntWritable> levelIteratorForKey = levelEntry.getValue();

      assertEquals(dataEntry.getKey(), mapDBEntry.getKey());
//      assertEquals(dataEntry.getKey(), levelEntry.getKey());

      for (IntWritable expectedValue : dataEntry.getValue()) {
        assertEquals(expectedValue, mapIteratorForKey.next());
//        assertEquals(expectedValue, levelIteratorForKey.next());
      }
    }
  }
}
