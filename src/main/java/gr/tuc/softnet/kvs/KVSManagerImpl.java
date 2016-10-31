package gr.tuc.softnet.kvs;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import gr.tuc.softnet.core.InjectorUtils;
import gr.tuc.softnet.core.LQPConfiguration;
import gr.tuc.softnet.core.NodeManager;
import gr.tuc.softnet.netty.MCDataTransport;
import gr.tuc.softnet.netty.messages.KVSDescriptionRequest;
import gr.tuc.softnet.netty.messages.KVSDescriptionResponse;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

/**
 * Created by vagvaz on 22/02/16.
 * The KVSManagerImpl should have a dataTransport reference in order to execute remote calls of create/destroy
 * The Interanal structure should be able to handle efficiently the get/Create KVS with minimal impact in memory
 * so there should be a KVSProxy in to the structure, each KVS is distinguished by the name
 * It should also be able to handle unmaterialized kvs (pipeline scenearios)
 * Thus a map datastructure with KVSWrapper( KVSProxy,Configuration,Observerable)
 */
@Singleton public class KVSManagerImpl implements KVSManager {
  @Inject MCDataTransport dataTransport;

  @Inject NodeManager nodeManager;
  Map<String, KVSWrapper> kvsWrapperMap;
  List<String> nodeNames;
  KVSFactory factory;



  public void initialize() {
    factory = new KVSFactory(dataTransport.getConfiguration());
    kvsWrapperMap = new HashedMap();
    nodeNames = nodeManager.getNodeNames();
  }

  public <K extends WritableComparable, V extends Writable> KeyValueStore<K, V> createKVS(
    String name, Configuration configuration) {
    KVSConfiguration kvsConfiguration = new KVSConfiguration(configuration);
    kvsConfiguration.setName(name);
    String node = nodeManager.resolveNode(name);
    kvsConfiguration = dataTransport.remoteCreateKVS(node, kvsConfiguration);
    bootstrapRemoteKVS(kvsConfiguration);
    return find(name);
  }

  @Override public KVSConfiguration remoteCreateKVS(String name, KVSConfiguration configuration) {
    KVSConfiguration kvsConfiguration = new KVSConfiguration(configuration);
    kvsConfiguration.setName(name);
    String node = nodeManager.resolveNode(name);
    if (!node.equals(dataTransport.getConfiguration().getURI() )) {
      System.err.println(
        "ERROR: remoteCreate " + name + " is not handled by me " + dataTransport.getConfiguration().getURI() + " but "
          + node);
      kvsConfiguration = dataTransport.remoteCreateKVS(node, configuration);
      return kvsConfiguration;
    } else {
      return dataTransport.createKVS(name, configuration);
    }
  }

  private <K extends WritableComparable, V extends Writable> KeyValueStore<K, V> find(String name) {
    KVSProxy<K, V> result = null;
    KVSWrapper<K, V> wrapper = kvsWrapperMap.get(name);
    if (wrapper != null) {
      result = wrapper.getProxy();
    } else {
      KVSDescriptionRequest request = new KVSDescriptionRequest(name);
      String kvsStoreMaster = nodeManager.resolveNode(name);
      KVSDescriptionResponse response = dataTransport.getKVSInfo(kvsStoreMaster, request);
      KVSConfiguration kvsConfiguration = response.getConfiguration();
      if (kvsConfiguration.getName().equals(name)) {
        bootstrapRemoteKVS(kvsConfiguration);
        return find(name);
      }
    }
    return result;
  }

  @Override public KVSConfiguration bootstrapKVS(KVSConfiguration kvsConfiguration) {
    if (this.kvsWrapperMap.containsKey(kvsConfiguration.getName())) {
      return kvsWrapperMap.get(kvsConfiguration.getName()).getConfiguration();
    }
    KeyValueStore newKVS = factory.createNewInstance(kvsConfiguration);
    Class<? extends MCPartitioner> partitionerClass = kvsConfiguration.getPartitionerClass();
    MCPartitioner partitioner = null;
    try {
      partitioner = partitionerClass.getConstructor().newInstance();
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      e.printStackTrace();
    } finally {
      if (partitioner == null) {
        partitioner = new DefaultHashPartitioner();
      }
    }
    kvsConfiguration.setPartitionerClass(partitioner.getClass());
    partitioner.initialize(kvsConfiguration.getPartitionerConf(), dataTransport);
    KVSProxy kvsProxy = InjectorUtils.getInjector().getInstance(KVSProxy.class);
    kvsProxy.initialize(kvsConfiguration.getName(), partitioner, kvsConfiguration);

    KVSWrapper wrapper = new KVSWrapper(newKVS, kvsProxy, kvsConfiguration);
    kvsWrapperMap.put(kvsConfiguration.getName(), wrapper);
    return kvsConfiguration;
  }

  @Override public Class<? extends WritableComparable> getKeyClass(KeyValueStore store) {
    KVSWrapper wrapper = kvsWrapperMap.get(store.getName());
    if (wrapper == null) {
      return null;
    }
    return (Class<? extends WritableComparable>) wrapper.getConfiguration().getKeyClass();
  }

  @Override public Class<? extends Writable> getValueClass(KeyValueStore store) {
    KVSWrapper wrapper = kvsWrapperMap.get(store.getName());
    if (wrapper == null) {
      return null;
    }
    return (Class<? extends Writable>) wrapper.getConfiguration().getValueClass();
  }

  private void bootstrapRemoteKVS(KVSConfiguration kvsConfiguration) {
    if (kvsWrapperMap.containsKey(kvsConfiguration.getName())) {
      return;
    }
    Class<? extends MCPartitioner> partitionerClass = kvsConfiguration.getPartitionerClass();
    MCPartitioner partitioner = null;
    try {
      partitioner = partitionerClass.getConstructor().newInstance();
    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      e.printStackTrace();
    } finally {
      if (partitioner == null) {
        partitioner = new DefaultHashPartitioner();
      }
    }
    partitioner.initialize(kvsConfiguration.getPartitionerConf(), dataTransport);
    KVSProxy kvsProxy = InjectorUtils.getInjector().getInstance(KVSProxy.class);
    kvsProxy.initialize(kvsConfiguration.getName(), partitioner, kvsConfiguration);
    KVSWrapper wrapper = new KVSWrapper(null, kvsProxy, kvsConfiguration);
    kvsWrapperMap.put(kvsConfiguration.getName(), wrapper);

  }

  public boolean destroyKVS(String name) {
    KVSWrapper wrapper = kvsWrapperMap.get(name);
    wrapper.proxy.close();
    wrapper.kvs.close();

    return true;
  }


  public <K extends WritableComparable, V extends Writable> KeyValueStore<K, V> getKVS(
    String name) {
    KeyValueStore<K, V> result = null;
    KVSWrapper<K, V> wrapper = kvsWrapperMap.get(name);
    if (wrapper != null) {
      result = wrapper.getKVS();
    }
    return result;
  }

  public <K extends WritableComparable, V extends Writable> KVSProxy<K, V> getKVSProxy(
    String name) {
    KVSProxy<K, V> result = null;
    KVSWrapper<K, V> wrapper = kvsWrapperMap.get(name);
    if (wrapper != null) {
      result = wrapper.getProxy();
    }
    return result;
  }

  @Override public KVSConfiguration getKVSConfiguration(String name) {
    KVSWrapper wrapper = kvsWrapperMap.get(name);
    if (wrapper != null) {
      return wrapper.getConfiguration();
    }
    return null;
  }

}
