package gr.tuc.softnet.kvs;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import gr.tuc.softnet.core.InjectorUtils;
import gr.tuc.softnet.netty.MCDataTransport;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.Map;

/**
 * Created by vagvaz on 22/02/16.
 * The KVSManagerImpl should have a dataTransport reference in order to execute remote calls of create/destroy
 * The Interanal structure should be able to handle efficiently the get/Create KVS with minimal impact in memory
 * so there should be a KVSProxy in to the structure, each KVS is distinguished by the name
 * It should also be able to handle unmaterialized kvs (pipeline scenearios)
 * Thus a map datastructure with KVSWrapper( KVSProxy,Configuration,Observerable)
 */
@Singleton
public class KVSManagerImpl implements KVSManager {
    @Inject
    MCDataTransport dataTransport;
    Map<String,KVSWrapper> kvsWrapperMap;
    KVSFactory factory;


    public void initialize(){
        factory = new KVSFactory(dataTransport.getConfiguration());
        kvsWrapperMap = new HashedMap();
    }

    public <K extends WritableComparable, V extends Writable> KeyValueStore<K, V> createKVS(String name, Configuration configuration) {
        KVSConfiguration kvsConfiguration = new KVSConfiguration(configuration);
        kvsConfiguration.setName(name);
        bootstrapKVS(kvsConfiguration);
        if(kvsConfiguration.isLocal()) {
            return find(name);
        }
        dataTransport.createKVS(name,kvsConfiguration);
        return find(name);
    }

    private <K extends WritableComparable, V extends Writable> KeyValueStore<K, V> find(String name) {
        KVSProxy<K,V> result = null;
        KVSWrapper<K,V> wrapper= kvsWrapperMap.get(name);
        if(wrapper != null){
            result = wrapper.getProxy();
        }
        return  result;
    }


    private <K extends WritableComparable, V extends Writable> void bootstrapKVS(KVSConfiguration kvsConfiguration) {
        KeyValueStore<K,V> newKVS = factory.createNewInstance(kvsConfiguration);
        MCPartitioner partitioner = null;
//        if(kvsConfiguration.getPartioner() == null){
            partitioner = new DefaultHashPartitioner();
//        }
//        else{
//            partitioner = PartitionerFactory.getParti
//        }
        KVSProxy kvsProxy = InjectorUtils.getInjector().getInstance(KVSProxy.class);
        kvsProxy.initialize(kvsConfiguration.getName(),partitioner);
        KVSWrapper<K,V> wrapper = new KVSWrapper<K, V>(newKVS,kvsProxy);
        kvsWrapperMap.put(kvsConfiguration.getName(),wrapper);
    }


    public boolean destroyKVS(String name) {
        KVSWrapper wrapper = kvsWrapperMap.get(name);
        wrapper.proxy.close();
        wrapper.kvs.close();

        return true;
    }


    public <K extends WritableComparable, V extends Writable> KeyValueStore<K, V> getKVS(String name) {
        KeyValueStore<K,V> result = null;
                KVSWrapper<K,V> wrapper= kvsWrapperMap.get(name);
        if(wrapper != null){
            result = wrapper.getKVS();
        }
        return result;
    }

    public <K extends WritableComparable, V extends Writable> KVSProxy<K, V> getKVSProxy(String name) {
        KVSProxy<K,V> result = null;
        KVSWrapper<K,V> wrapper= kvsWrapperMap.get(name);
        if(wrapper != null){
            result = wrapper.getProxy();
        }
        return result;
    }
}
