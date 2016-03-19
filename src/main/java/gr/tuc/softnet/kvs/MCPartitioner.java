package gr.tuc.softnet.kvs;

import org.apache.commons.configuration.Configuration;

/**
 * Created by vagvaz on 16/02/16.
 */
public interface MCPartitioner {
    void setConfiguration(Configuration configuration);
    void updateConfiguration(Configuration configuration);
    Configuration getConfiguration();
    int partition(Object key);
    int partition(Object key, int numberOfBuckets);
}
