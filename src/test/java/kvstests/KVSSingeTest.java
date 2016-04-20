package kvstests;

import junit.framework.TestCase;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import gr.tuc.softnet.kvs.KVSConfiguration;
import gr.tuc.softnet.kvs.LevelDBSignleKVS;
import gr.tuc.softnet.kvs.MapDBSingleKVS;
import rx.Observable;
import rx.Subscriber;

/**
 * Created by ap0n on 8/4/2016.
 */
public class KVSSingeTest extends TestCase {

  protected MapDBSingleKVS<IntWritable, IntWritable> mapdbKVS;
  protected LevelDBSignleKVS<IntWritable, IntWritable> leveldbKVS;
  protected KVSConfiguration mapDBConfiguration;
  protected KVSConfiguration levelDBConfiguration;

  protected String mapKVSName;
  protected String levelKVSName;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mapKVSName = "mapKVSName";
    levelKVSName = "levelKVSName";

    mapDBConfiguration = new KVSConfiguration();
    mapDBConfiguration.setName(mapKVSName);
    mapDBConfiguration.setBaseDir("/tmp/mapdb/");
    mapDBConfiguration.setKeyClass(IntWritable.class);
    mapDBConfiguration.setValueClass(IntWritable.class);

    levelDBConfiguration = new KVSConfiguration();
    levelDBConfiguration.setName(levelKVSName);
    levelDBConfiguration.setBaseDir("/tmp/leveldb/");
    levelDBConfiguration.setKeyClass(IntWritable.class);
    levelDBConfiguration.setValueClass(IntWritable.class);

    mapdbKVS = new MapDBSingleKVS<>(mapDBConfiguration);
    leveldbKVS = new LevelDBSignleKVS<>(levelDBConfiguration);
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    mapdbKVS.close();
    leveldbKVS.close();
  }

  @Test
  public void testGetName() {
    System.out.println("KVSSingeTest.testGetName");
    assertEquals(mapKVSName, mapdbKVS.getName());
    assertEquals(levelKVSName, leveldbKVS.getName());
  }

  @Test
  public void testPutAndGet() {
    System.out.println("KVSSingeTest.testPutAndGet");
    Map<IntWritable, IntWritable> data = new HashMap<>();
    for (int i = 0; i < 1000; i++) {
      data.put(new IntWritable(i), new IntWritable(i));
    }

    try {
      putToBothKVSs(data);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
    try {
      readFromBothKVSs(data);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSize() {
    System.out.println("KVSSingeTest.testSize");
    Map<IntWritable, IntWritable> data = new HashMap<>();
    int size = 1000;
    for (int i = 0; i < size; i++) {
      data.put(new IntWritable(i), new IntWritable(i));
    }

    try {
      putToBothKVSs(data);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

    assertEquals(size, mapdbKVS.size());
    assertEquals(size, leveldbKVS.size());
  }

  @Test
  public void testContains() {
    System.out.println("KVSSingeTest.testContains");
    Map<IntWritable, IntWritable> data = new HashMap<>();
    int size = 1000;
    for (int i = 0; i < size; i++) {
      data.put(new IntWritable(i), new IntWritable(i));
    }

    try {
      putToBothKVSs(data);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

    for (int i = 0; i < size; i++) {
      assertTrue(mapdbKVS.contains(new IntWritable(i)));
      assertTrue(leveldbKVS.contains(new IntWritable(i)));
    }
  }

  @Test
  public void testIterator() {
    System.out.println("KVSSingeTest.testIterator");
    Map<IntWritable, IntWritable> data = new HashMap<>();
    int size = 1000;
    for (int i = 0; i < size; i++) {
      data.put(new IntWritable(i), new IntWritable(i));
    }

    try {
      putToBothKVSs(data);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

    Iterator<Map.Entry<IntWritable, IntWritable>> it = leveldbKVS.iterator().iterator();

    for (int j = 0; j < 2; j++) {  // run for both KVSs
      for (int i = 0; i < size; i++) {
        assertTrue(it.hasNext());
        Map.Entry<IntWritable, IntWritable> entry = it.next();
        assertEquals(i, entry.getKey().get());
        assertEquals(i, entry.getValue().get());
      }
      assertFalse(it.hasNext());
      it = mapdbKVS.iterator().iterator();
    }
  }

  @Test
  public void testIteratorFromObserver() {
    System.out.println("KVSSingeTest.testIteratorFromObserver");
    Map<IntWritable, IntWritable> data = new HashMap<>();
    int size = 1000;
    for (int i = 0; i < size; i++) {
      data.put(new IntWritable(i), new IntWritable(i));
    }

    try {
      putToBothKVSs(data);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
    Observable.from(mapdbKVS.iterator()).subscribe(new TestObserver(data));
    Observable.from(leveldbKVS.iterator()).subscribe(new TestObserver(data));
  }

  private class TestObserver extends Subscriber<Map.Entry<IntWritable, IntWritable>> {

    Map<IntWritable, IntWritable> expectedData;

    public TestObserver(Map<IntWritable, IntWritable> expectedData) {
      this.expectedData = new HashMap<>(expectedData);
    }

    @Override
    public void onCompleted() {
      assertTrue(expectedData.isEmpty());
    }

    @Override
    public void onError(Throwable e) {
      fail();
    }

    @Override
    public void onNext(Map.Entry<IntWritable, IntWritable> intWritableIntWritableEntry) {

      assertTrue(expectedData.containsKey(intWritableIntWritableEntry.getKey()));
      expectedData.remove(intWritableIntWritableEntry.getKey());
    }
  }

  private void putToBothKVSs(Map<IntWritable, IntWritable> data) throws Exception {
    for (Map.Entry<IntWritable, IntWritable> e : data.entrySet()) {
      mapdbKVS.put(e.getKey(), e.getValue());
      leveldbKVS.put(e.getKey(), e.getValue());
    }
  }

  private void readFromBothKVSs(Map<IntWritable, IntWritable> data) {
    for (Map.Entry<IntWritable, IntWritable> e : data.entrySet()) {
      assertEquals(e.getValue().get(), mapdbKVS.get(e.getKey()).get());
      assertEquals(e.getValue().get(), leveldbKVS.get(e.getKey()).get());
    }
  }
}
