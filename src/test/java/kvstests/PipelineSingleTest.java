package kvstests;

import junit.framework.TestCase;

import org.apache.hadoop.io.IntWritable;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;

import gr.tuc.softnet.kvs.KVSConfiguration;
import gr.tuc.softnet.kvs.PipelineSingleKVS;
import rx.Observable;
import rx.Subscriber;

/**
 * Created by ap0n on 5/4/2016.
 */
public class PipelineSingleTest extends TestCase {
  protected PipelineSingleKVS<IntWritable, IntWritable> kvs;
  protected KVSConfiguration configuration;

  @Override
  @Before
  protected void setUp() throws Exception {
    super.setUp();
    configuration = new KVSConfiguration();
    configuration.setName("testName");
    configuration.setBatchSize(3);
    kvs = new PipelineSingleKVS<>(configuration);
  }

  @Test
  public void testGetName() {
    System.out.println("PipelineSingleTest.testGetName");
    assertEquals("testName", kvs.getName());
  }

  @Test
  public void testIterator() {
    System.out.println("PipelineSingleTest.testIterator");
    for (int i = 0; i < configuration.getBatchSize(); i++) {
      kvs.put(new IntWritable(i), new IntWritable(i));
    }
    Iterable<Map.Entry<IntWritable, IntWritable>> iterable = kvs.iterator();

    Iterator<Map.Entry<IntWritable, IntWritable>> iterator = iterable.iterator();

    IntWritable lastKey = new IntWritable(0);
    IntWritable lastValue = new IntWritable(0);

    while (iterator.hasNext()) {
      Map.Entry<IntWritable, IntWritable> e = iterator.next();
      assertEquals(lastKey, e.getKey());
      assertEquals(lastValue, e.getValue());
      lastKey.set(lastKey.get() + 1);
      lastValue.set(lastValue.get() + 1);
    }

    kvs.close();
  }

  @Test
  public void testPuts() {
    System.out.println("PipelineSingleTest.testPuts");
    Observable observable = Observable.create(kvs);
    observable.subscribe(new TestSubscriber());

    for (int i = 0; i < configuration.getBatchSize(); i++) {
      kvs.put(new IntWritable(i), new IntWritable(i));
    }
    kvs.close();
  }

  @Test
  public void testGets() {
    System.out.println("PipelineSingleTest.testGets");
    for (int i = 0; i < configuration.getBatchSize() - 1; i++) {  // don't let the kvs flush
      kvs.put(new IntWritable(i), new IntWritable(i));
    }

    for (int i = 0; i < configuration.getBatchSize() - 1; i++) {
      assertEquals(i, kvs.get(new IntWritable(i)).get());
    }
  }

  private class TestSubscriber extends Subscriber<Map.Entry<IntWritable, IntWritable>> {

    int onNextCounter;  // counts how many times onNext has been  called
    IntWritable lastKey;
    IntWritable lastValue;

    public TestSubscriber() {
      this.onNextCounter = 0;
      this.lastKey = new IntWritable(0);
      this.lastValue = new IntWritable(0);
    }

    @Override
    public void onCompleted() {
      assertEquals(configuration.getBatchSize(), onNextCounter);
    }

    @Override
    public void onError(Throwable e) {
      fail();
    }

    @Override
    public void onNext(Map.Entry<IntWritable, IntWritable> intWritableIntWritableEntry) {
      onNextCounter++;
      assertEquals(lastKey, intWritableIntWritableEntry.getKey());
      assertEquals(lastValue, intWritableIntWritableEntry.getValue());
      lastKey.set(lastKey.get() + 1);
      lastValue.set(lastValue.get() + 1);
    }
  }
}
