package kvstests;

import com.google.inject.Guice;
import com.google.inject.Injector;

import junit.framework.TestCase;

import org.apache.hadoop.io.IntWritable;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import gr.tuc.softnet.mapred.examples.DataLoader;
import gr.tuc.softnet.core.InjectorUtils;
import gr.tuc.softnet.core.MCNodeModule;
import gr.tuc.softnet.core.NodeManager;
import gr.tuc.softnet.core.StringConstants;
import gr.tuc.softnet.kvs.KVSManager;
import gr.tuc.softnet.kvs.KeyValueStore;

/**
 * Created by ap0n on 19/5/2016.
 */
public class TestDataLoader extends TestCase {

  private NodeManager nodeManager;
  private Injector injector;

  @Test
  public void testDataLoader() {

    injector = Guice.createInjector(new MCNodeModule());
    InjectorUtils.setInjector(injector);
    nodeManager = injector.getInstance(NodeManager.class);
    nodeManager.initialize(StringConstants.DEFAULT_CONF_DIR);  // TODO: Is the argument ok?

    int dataCount = 1000;
    String kvsName = "testDataInput";
    Map<IntWritable, IntWritable> data = generateData(dataCount);
    DataLoader<IntWritable, IntWritable> dataLoader = new DataLoader<>(kvsName,
                                                                       nodeManager,
                                                                       injector,
                                                                       IntWritable.class,
                                                                       IntWritable.class);
    try {
      dataLoader.load(data);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
    validateKvsData(data, kvsName);
  }

  private Map<IntWritable, IntWritable> generateData(int count) {
    Map<IntWritable, IntWritable> data = new HashMap<>();

    for (int i = 0; i < count; i++) {
      data.put(new IntWritable(i), new IntWritable(i));
    }
    return data;
  }

  private void validateKvsData(Map<IntWritable, IntWritable> expected, String kvsName) {
    KVSManager kvsManager = injector.getInstance(KVSManager.class);
    KeyValueStore<IntWritable, IntWritable> kvs = kvsManager.getKVS(kvsName);

    IntWritable r = kvs.get(new IntWritable(0));

    for (Map.Entry<IntWritable, IntWritable> e : expected.entrySet()) {
      Assert.assertEquals(e.getValue(), kvs.get(e.getKey()));
    }
  }
}
