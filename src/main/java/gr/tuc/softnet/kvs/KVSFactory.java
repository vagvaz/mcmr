package gr.tuc.softnet.kvs;

import com.google.inject.Inject;
import gr.tuc.softnet.core.ConfStringConstants;
import gr.tuc.softnet.core.MCConfiguration;
import gr.tuc.softnet.core.StringConstants;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by vagvaz on 25/02/16.
 */
public class KVSFactory {
  Logger logger = LoggerFactory.getLogger(KVSFactory.class);

  MCConfiguration systemConfiguration;

  public KVSFactory(MCConfiguration configuration) {
    this.systemConfiguration = configuration;
  }

  public <K extends WritableComparable, V extends Writable> KeyValueStore<K, V> createNewInstance(
      KVSConfiguration configuration) {
    KeyValueStore<K, V> result = null;

    String type = configuration.getString(ConfStringConstants.CACHE_TYPE,StringConstants.PIPELINE);
//        systemConfiguration.conf().getString(ConfStringConstants.CACHE_TYPE));
    if (!configuration.isMaterialized())
      type = StringConstants.PIPELINE;
    if (type.equals(StringConstants.MAPDB)) {
      logger.info(
          "Create " + configuration.getName() + " MapDB kvs in " + configuration.getBaseDir());
      result = new MapDBIndex<K, V>(configuration);
    } else if (type.equals(StringConstants.LEVELDB)) {
      logger.info(
          "Create " + configuration.getName() + " LevelDB kvs in " + configuration.getBaseDir());
      result = new LevelDBIndex<K, V>(configuration);
    } else if (type.equals(StringConstants.PIPELINE)) {
      logger.info(
          "Create " + configuration.getName() + " pipeline kvs in " + configuration.getBaseDir());
      result = new TestKVS<K, V>();//(configuration.defaultBaseDir,configuration.getName());
      //            result = new PipelinedKeyValueStore<K,V>(configuration.defaultBaseDir,configuration.getName());
    } else {
      logger.error("Create KVS instance with invalid type " + type);
    }
    return result;
  }

}
