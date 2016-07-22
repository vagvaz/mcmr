package gr.tuc.softnet.kvs;

import gr.tuc.softnet.netty.MCDataTransport;
import org.apache.commons.configuration.Configuration;

import java.io.Serializable;
import java.util.Properties;

/**
 * Created by vagvaz on 16/02/16.
 */
public interface MCPartitioner extends Serializable {
    void setConfiguration(Properties configuration);
    void updateConfiguration(Properties configuration);
    Properties getConfiguration();
    int partition(Object key);
    int partition(Object key, int numberOfBuckets);
    void initialize(Properties partitionerConf, MCDataTransport dataTransport);
    void setMCDataTransport(MCDataTransport transport);
}
