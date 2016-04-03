package gr.tuc.softnet.kvs;

import gr.tuc.softnet.core.ConfStringConstants;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.HierarchicalConfiguration;

/**
 * Created by vagvaz on 22/02/16.
 */
public class KVSConfiguration extends HierarchicalConfiguration {
    String defaultBaseDir = System.getProperty("java.io.tmpdir")+"/mcmcr/kvs/";
    boolean defaultIsLocal = false;


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
}
