package gr.tuc.softnet.kvs;

import gr.tuc.softnet.core.ConfStringConstants;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.HierarchicalConfiguration;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.*;

/**
 * Created by vagvaz on 22/02/16.
 */
public class KVSConfiguration extends HierarchicalConfiguration implements Serializable {
    String defaultBaseDir = System.getProperty("java.io.tmpdir")+"/mcmcr/kvs/";
    boolean defaultIsLocal = false;
    int batchSize;

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public KVSConfiguration(){
        super();
        setLocal(false);
    }

    public KVSConfiguration(String name){
        super();
        setLocal(false);
        setName(name);
        setBaseDir(System.getProperty("java.io.tmpdir")+"/mcmcr/kvs/");
    }

    public KVSConfiguration(Configuration configuration){
        super();
        this.append(configuration);

    }
    public void setCacheType(String type){
        setProperty(ConfStringConstants.CACHE_TYPE,type);
    }
    public String getCacheType(){
        return (String) getProperty(ConfStringConstants.CACHE_TYPE);
    }
    public boolean isMaterialized(){
        return getBoolean(ConfStringConstants.CACHE_MATERIALIZED,true);
    }
    public void setMaterialized(boolean materialized){
        setProperty(ConfStringConstants.CACHE_MATERIALIZED,materialized);
    }
    public boolean isLocal(){
        return getBoolean(ConfStringConstants.LOCAL_CACHE,defaultIsLocal);
    }

    public void setLocal(boolean local){
        setProperty(ConfStringConstants.LOCAL_CACHE,local);
    }

    public String getName(){
        return getString(ConfStringConstants.CACHE_NAME);
    }

    public void setName(String name){
        setProperty(ConfStringConstants.CACHE_NAME,name);
    }

    public String getBaseDir(){
        return getString(ConfStringConstants.KVS_BASE_DIR, defaultBaseDir);
    }

    public void setBaseDir(String baseDir){
        setProperty(ConfStringConstants.KVS_BASE_DIR,baseDir);
    }

    public Class<?> getValueClass() {
        return (Class<?>) getProperty(ConfStringConstants.VALUE_CLASS);
    }

    public Class<? extends Comparable> getKeyClass() {
        return (Class<? extends Comparable>) getProperty(ConfStringConstants.KEY_CLASS);
    }

    public void setValueClass(Class<?> valueClass) {
        setProperty(ConfStringConstants.VALUE_CLASS,valueClass);
    }

    public void setKeyClass(Class<? extends Comparable> keyClass) {
       setProperty(ConfStringConstants.KEY_CLASS,keyClass);
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        Iterator<String> iterator =  this.getKeys();
        int counter = 0;
        this.getRootNode().getChildren().size();

        List<Map.Entry<String,Serializable>> list = new LinkedList<>();
        while(iterator.hasNext()){

            String key = iterator.next();
            list.add(new AbstractMap.SimpleEntry<String, Serializable>(key,
              (Serializable) this.getProperty(key)));
        }
        out.writeInt(list.size());
        for(Map.Entry<String,Serializable> entry : list){
            out.writeObject(entry.getKey());
            out.writeObject(entry.getValue());
        }

    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        int sz = in.readInt();
        while(sz > 0){
            sz--;
            String key = (String) in.readObject();
            Object value = in.readObject();
            this.setProperty(key,value);
        }
    }

    private void readObjectNoData() throws ObjectStreamException {

    }
}
