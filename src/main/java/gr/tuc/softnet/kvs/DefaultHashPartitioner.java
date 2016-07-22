package gr.tuc.softnet.kvs;

import gr.tuc.softnet.core.StringConstants;
import gr.tuc.softnet.netty.MCDataTransport;
import org.apache.commons.configuration.Configuration;

import java.util.Iterator;
import java.util.Properties;

/**
 * Created by vagvaz on 16/02/16.
 */
public class DefaultHashPartitioner implements MCPartitioner {
  Properties configuration;
  int numberOfBuckets;
  private volatile MCDataTransport transport;

  @Override public void setConfiguration(Properties configuration) {
    this.configuration = configuration;
    if (this.configuration.containsKey(StringConstants.NUM_BUCKETS)) {
      numberOfBuckets =
        Integer.parseInt(this.configuration.getProperty(StringConstants.NUM_BUCKETS));
    } else {
      numberOfBuckets = 1;
    }
  }

  @Override public void updateConfiguration(Properties configuration) {
    Iterator<Object> iterator = configuration.keySet().iterator();
    while (iterator.hasNext()) {
      String current = (String) iterator.next();
      this.configuration.put(current, configuration.get(current));
    }
  }

  @Override public Properties getConfiguration() {
    return configuration;
  }

  @Override public int partition(Object key) {
    return Math.abs(key.hashCode()) % numberOfBuckets;
  }

  @Override public int partition(Object key, int numberOfBuckets) {
    return Math.abs(key.hashCode()) % numberOfBuckets;
  }

  @Override public void initialize(Properties partitionerConf, MCDataTransport dataTransport) {
    this.transport = dataTransport;
    setConfiguration(partitionerConf);
  }

  @Override public void setMCDataTransport(MCDataTransport transport) {
    this.transport = transport;
  }

}
