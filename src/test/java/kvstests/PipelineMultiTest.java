package kvstests;

import junit.framework.TestCase;

import org.apache.hadoop.io.IntWritable;

import java.util.Iterator;
import java.util.Map;

import gr.tuc.softnet.kvs.KVSConfiguration;
import gr.tuc.softnet.kvs.PipelineMultiKVS;
import rx.Observable;
import rx.Subscriber;

/**
 * Created by ap0n on 5/4/2016.
 */
public class PipelineMultiTest extends TestCase {

  protected PipelineMultiKVS<IntWritable, IntWritable> kvs;
  protected KVSConfiguration configuration;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    configuration = new KVSConfiguration();
    configuration.setName("testName");
    configuration.setBatchSize(1000);
    kvs = new PipelineMultiKVS<>(configuration);
  }

  public void testAppends() {
    System.out.println("Testing appends");
    Observable observable = Observable.create(kvs);
    observable.subscribe(new PipelineMultiTest.TestSubscriber());

    for (int k = 0; k < configuration.getBatchSize() / 2; k++) {
      for (int v = 0; v < configuration.getBatchSize() * 3; v++) {
        kvs.append(new IntWritable(k), new IntWritable(v));
      }
    }
    kvs.close();
  }

  private class TestSubscriber extends Subscriber<Map.Entry<IntWritable, Iterator<IntWritable>>> {

    int valuesCounter;  // counts how many times onNext has been  called
    int lastKey;
    int lastValue;
    int expectedKey;
    int expectedValue;

    public TestSubscriber() {
      this.valuesCounter = 0;
      this.lastKey = 0;
      this.lastValue = 0;
      this.expectedKey = 0;
      this.expectedValue = 0;
    }

    @Override
    public void onCompleted() {
      assertEquals(configuration.getBatchSize() / 2 * configuration.getBatchSize() * 3,
                   valuesCounter);
    }

    @Override
    public void onError(Throwable e) {
      fail();
    }

    @Override
    public void onNext(Map.Entry<IntWritable, Iterator<IntWritable>> intWritableIntWritableEntry) {
      Iterator<IntWritable> iter =  intWritableIntWritableEntry.getValue();

      while (iter.hasNext()) {
        assertEquals(expectedKey, intWritableIntWritableEntry.getKey().get());
        assertEquals(expectedValue++, iter.next().get());

        if (++valuesCounter % (configuration.getBatchSize() * 3) == 0) {
          expectedKey++;
          expectedValue = 0;
        }
      }
    }
  }
}
