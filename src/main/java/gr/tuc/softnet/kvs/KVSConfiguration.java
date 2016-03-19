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
    boolean isMaterialized(){
        return getBoolean(ConfStringConstants.CACHE_MATERIALIZED,true);
    }
    void setMaterialized(boolean materialized){
        setProperty(ConfStringConstants.CACHE_MATERIALIZED,materialized);
    }
    boolean isLocal(){
        return getBoolean(ConfStringConstants.LOCAL_CACHE,defaultIsLocal);
    }

    void setLocal(boolean local){
        setProperty(ConfStringConstants.LOCAL_CACHE,local);
    }

    String getName(){
        return getString(ConfStringConstants.CACHE_NAME);
    }

    void setName(String name){
        setProperty(ConfStringConstants.CACHE_NAME,name);
    }

    String getBaseDir(){
        return getString(ConfStringConstants.KVS_BASE_DIR, defaultBaseDir);
    }

    void setBaseDir(String baseDir){
        setProperty(ConfStringConstants.KVS_BASE_DIR,baseDir);
    }

    public String getValueClass() {
        return getString(ConfStringConstants.VALUE_CLASS);
    }

    public String getKeyClass() {
        return getString(ConfStringConstants.KEY_CLASS);
    }

    public void setValueClass(String valueClass) {
        setProperty(ConfStringConstants.VALUE_CLASS,valueClass);
    }

    public void setKeyClass(String keyClass) {
       setProperty(ConfStringConstants.KEY_CLASS,keyClass);
    }
}
