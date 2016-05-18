package gr.tuc.softnet.engine;

import gr.tuc.softnet.core.ConfStringConstants;
import gr.tuc.softnet.mapred.MCMapper;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.*;

/**
 * Created by vagvaz on 03/03/16.
 */
public class JobConfiguration extends HierarchicalConfiguration implements Serializable {
    public JobConfiguration(){
        super();
    }


    public Class getMapOutputKeyClass() {
        return (Class) getProperty(ConfStringConstants.MAP_OUTPUT_KEY_CLASS);
    }

    public void setMapOutputKeyClass(Class keyClass) {
        setProperty(ConfStringConstants.MAP_OUTPUT_KEY_CLASS, keyClass);
    }

    public Class getMapOutputValueClass() {
        return (Class) getProperty(ConfStringConstants.MAP_OUTPUT_VALUE_CLASS);
    }

    public void setMapOutputValueClass(Class valueClass) {
        setProperty(ConfStringConstants.MAP_OUTPUT_VALUE_CLASS, valueClass);
    }

    public Class getLocalReduceOutputKeyClass() {
        return (Class) getProperty(ConfStringConstants.LOCAL_REDUCE_OUTPUT_KEY_CLASS);
    }

    public void setLocalReduceOutputKeyClass(Class keyClass) {
        setProperty(ConfStringConstants.LOCAL_REDUCE_OUTPUT_KEY_CLASS, keyClass);
    }

    public Class getLocalReduceOutputValueClass() {
        return (Class) getProperty(ConfStringConstants.LOCAL_REDUCE_OUTPUT_VALUE_CLASS);
    }

    public void setLocalReduceOutputValueClass(Class valueClass) {
        setProperty(ConfStringConstants.LOCAL_REDUCE_OUTPUT_VALUE_CLASS, valueClass);
    }

    public Class getFederationReduceOutputKeyClass() {
        return (Class) getProperty(ConfStringConstants.FEDERATION_REDUCE_OUTPUT_KEY_CLASS);
    }

    public void setFederationReduceOutputKeyClass(Class keyClass) {
        setProperty(ConfStringConstants.FEDERATION_REDUCE_OUTPUT_KEY_CLASS, keyClass);
    }

    public Class getFederationReduceOutputValueClass() {
        return (Class) getProperty(ConfStringConstants.FEDERATION_REDUCE_OUTPUT_VALUE_CLASS);
    }

    public void setFederationReduceOutputValueClass(Class valueClass) {
        setProperty(ConfStringConstants.FEDERATION_REDUCE_OUTPUT_VALUE_CLASS, valueClass);
    }
    
    public String getOutput() {
        return getString(ConfStringConstants.TASK_OUTPUT);
    }

    public void setOutput(String output) {
        setProperty(ConfStringConstants.TASK_OUTPUT, output);
    }

    public void setInput(String input) {
        setProperty(ConfStringConstants.TASK_INPUT, input);
    }

    public String getInput() {
        return getString(ConfStringConstants.TASK_INPUT);
    }

    public Class<? extends Reducer<?, ?, ?, ?>> getFederationReducerClass() {
        return (Class<? extends Reducer<?, ?, ?, ?>>) getProperty(ConfStringConstants.FEDERATION_REDUCER_CLASS);
    }

    public Class<? extends Reducer<?, ?, ?, ?>> getLocalReducerClass() {
        return (Class<? extends Reducer<?, ?, ?, ?>>) getProperty(ConfStringConstants.LOCAL_REDUCER_CLASS);
    }

    public void setMapperClass(Class<? extends MCMapper> mapperClass) {
        setProperty(ConfStringConstants.MAP_CLASS, mapperClass);
    }
    public Class<? extends MCMapper> getMapperClass()
    {
        return (Class<? extends MCMapper>) getProperty(ConfStringConstants.MAP_CLASS);
    }

    public String getJar() {
        return getString(ConfStringConstants.JAR_NAME);
    }

    public void setJar(String jar) {
        setProperty(ConfStringConstants.JAR_NAME, jar);
    }

    public Class<? extends Partitioner<?, ?>> getPartitionerClass() {
        return (Class<? extends Partitioner<?, ?>>) getProperty(ConfStringConstants.PARTITIONER_CLASS);
    }

    public void setPartitionerClass(Class<? extends Partitioner<?, ?>> partionerClass) {
        setProperty(ConfStringConstants.PARTITIONER_CLASS, partionerClass);
    }

    public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() {
        return (Class<? extends Reducer<?, ?, ?, ?>>) getProperty(ConfStringConstants.COMBINER_CLASS);
    }

    public void setCombinerClass(Class<? extends Reducer<?, ?, ?, ?>> combinerClass) {
        setProperty(ConfStringConstants.COMBINER_CLASS, combinerClass);
    }

    public void setIsMapPipeline(boolean mapPipeline){
        setProperty(ConfStringConstants.IS_MAP_PIPELINE,mapPipeline);
    }
    public boolean isMapPipeline(){
        return getBoolean(ConfStringConstants.IS_MAP_PIPELINE,false);
    }

    public void setIsLocalReducePipeline(boolean localReducePipeline){
        setProperty(ConfStringConstants.IS_LOCAL_REDUCE_PIPELINE,localReducePipeline);
    }
    public boolean isLocalReducePipeline(){
        return getBoolean(ConfStringConstants.IS_LOCAL_REDUCE_PIPELINE,false);
    }

    public void setIsFederationReducePipeline(boolean federationReducePipeline){
        setProperty(ConfStringConstants.IS_FEDERATION_REDUCE_PIPELINE,federationReducePipeline);
    }
    public boolean isFederationReducePipeline(){
        return getBoolean(ConfStringConstants.IS_FEDERATION_REDUCE_PIPELINE,false);
    }

    public void setJobProperty(String key, Serializable propertyValue){
        Map<String,Serializable> configMap =
          (Map<String, Serializable>) this.getProperty(ConfStringConstants.USER_JOB_CONFIG);
        if(configMap == null){
            configMap = new HashMap<>();
            this.setProperty(ConfStringConstants.USER_JOB_CONFIG,configMap);
        }
        configMap.put(key,propertyValue);
    }

    public Object getJobProperty(String key){
        Map<String,Serializable> configMap =
          (Map<String, Serializable>) this.getProperty(ConfStringConstants.USER_JOB_CONFIG);
        if(configMap == null){
            configMap = new HashMap<>();
            this.setProperty(ConfStringConstants.USER_JOB_CONFIG,configMap);
            return null;
        }
        return configMap.get(key);
    }

    public Map<String,Serializable> getJobProperties(){
        Map<String,Serializable> configMap =
          (Map<String, Serializable>) this.getProperty(ConfStringConstants.USER_JOB_CONFIG);
        if(configMap == null){
            configMap = new HashMap<>();
            this.setProperty(ConfStringConstants.USER_JOB_CONFIG,configMap);
        }
        return configMap;
    }

    public void setClouds(ArrayList<String> clouds){
        String property = "";
        for(String cloud : clouds){
            property += cloud+",";
        }
        setProperty(ConfStringConstants.CLOUD_LIST,property.substring(0,property.length()-1));
    }
    public void appendCloud(String cloud){
        String value = getString(ConfStringConstants.CLOUD_LIST);
        value+=","+cloud;
        setProperty(ConfStringConstants.CLOUD_LIST,value);
    }
    public List<String> getClouds() {
        List<Object> configList =  getList(ConfStringConstants.CLOUD_LIST,null);
        if(configList != null){
            Set<String> resultSet = new HashSet<>();
            for(Object microCloud : configList){
                resultSet.add(microCloud.toString());
            }
            List<String> result = new ArrayList<>(resultSet);
            return result;
        }
        return null;
    }

    public boolean hasLocalReduce() {
        return getLocalReducerClass() != null;
    }

    public String getJobID() {
        return getString(ConfStringConstants.JOB_ID);
    }

    public void setJobID(String id){
        setProperty(ConfStringConstants.JOB_ID,id);
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        Iterator<String> iterator =  this.getKeys();
        int counter = this.getRootNode().getChildren().size();
        out.writeInt(counter);
        while(iterator.hasNext()){
            if(counter-- < 0)
            {
                try {
                    throw new Exception("counter neg");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            String key = iterator.next();
            out.writeObject(key);
            out.writeObject(this.getProperty(key));
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
