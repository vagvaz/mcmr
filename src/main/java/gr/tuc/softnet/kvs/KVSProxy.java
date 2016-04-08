package gr.tuc.softnet.kvs;

import com.google.inject.Inject;
import gr.tuc.softnet.core.NodeManager;
import gr.tuc.softnet.netty.MCDataTransport;
import org.apache.commons.collections.FastTreeMap;
import rx.Subscriber;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 * Created by vagvaz on 16/02/16.
 */
public class KVSProxy<K,V> implements KeyValueStore<K,V> {
    String name;
    MCPartitioner partitioner;
    NodeManager nodeManager;
    List<String> nodeNames;
    @Inject
    MCDataTransport dataTransport;
    @Inject
    KVSManager kvsManager;
    SortedMap<Integer,MCDataBuffer> dataBuffers;

    public KVSProxy() {

    }

    public void initialize(String name , MCPartitioner partitioner){
        this.name = name;
        dataBuffers = new FastTreeMap();
        this.partitioner = partitioner;
    }

    public void put(K key, V value) {
        int index = partitioner.partition(key,nodeNames.size());
        if(dataBuffers.get(index).append(key,value)){
            flush(index);
        }
    }

    private void flush(int index) {
        dataBuffers.get(index).flush();
    }

    public void flush(){
        for(Map.Entry<Integer,MCDataBuffer> entry : dataBuffers.entrySet()){
            entry.getValue().flush();
        }
    }


    public V get(K key) {
        int index = partitioner.partition(key,nodeNames.size());
        V result = null;
        try{
            result = dataTransport.remoteGet(nodeNames.get(index),key);
        }catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }

    public int size() {
        int result = 0;
        KeyValueStore localKVS = kvsManager.getKVS(name);
        result = localKVS.size();
        return result;
    }

    @Override
    public Iterable<Map.Entry<K, Integer>> getKeysIterator() {
        return keysIterator();
    }


    public Iterable<Map.Entry<K, Integer>> keysIterator() {
        KeyValueStore localKVS = kvsManager.getKVS(name);
        return localKVS.getKeysIterator();
    }

    public int totalSize(){
        int result = 0;
        for(String node : nodeNames){
            result += dataTransport.remoteSize(node);
        }
        return result;
    }


    @Override
    public Iterator<V> getKeyIterator(K key, Integer counter) {
        KeyValueStore localKVS = kvsManager.getKVS(name);
        return localKVS.getKeyIterator(key,counter);
    }



    public Iterable<Map.Entry<K, V>> iterator() {
        KeyValueStore localKVS = kvsManager.getKVS(name);
        return localKVS.iterator();
    }

    public boolean contains(K key) {
        int index = partitioner.partition(key,nodeNames.size());
        boolean result = false;
        try{
            result = dataTransport.remoteContains(nodeNames.get(index),key);
        }catch(Exception e){
            e.printStackTrace();
        }
        return result;
    }


    public void close() {
        nodeNames.clear();
        for(Map.Entry<Integer,MCDataBuffer> entry : dataBuffers.entrySet()){
//            entry.getValue().flush();
            entry.getValue().clear();
        }
        dataBuffers.clear();
    }

    public String getName() {
        return name;
    }

    @Override
    public void call(Subscriber<? super Map.Entry<K, V>> subscriber) {

    }
}
