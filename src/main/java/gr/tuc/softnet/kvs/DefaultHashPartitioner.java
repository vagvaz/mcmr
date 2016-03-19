package gr.tuc.softnet.kvs;

import gr.tuc.softnet.core.StringConstants;
import org.apache.commons.configuration.Configuration;

import java.util.Iterator;

/**
 * Created by vagvaz on 16/02/16.
 */
public class DefaultHashPartitioner implements MCPartitioner {
    Configuration configuration;
    int numberOfBuckets;
    @Override
    public void setConfiguration(Configuration configuration) {
        this.configuration  = configuration;
        numberOfBuckets = this.configuration.getInt(StringConstants.NUM_BUCKETS,1);
    }

    @Override
    public void updateConfiguration(Configuration configuration) {
        Iterator<String> iterator = configuration.getKeys();
        while (iterator.hasNext()){
            String current = iterator.next();
            this.configuration.addProperty(current,configuration.getString(current));
        }
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public int partition(Object key) {
        return Math.abs(key.hashCode()) %  numberOfBuckets;
    }

    @Override
    public int partition(Object key, int numberOfBuckets) {
        return Math.abs(key.hashCode()) %  numberOfBuckets;
    }
}
