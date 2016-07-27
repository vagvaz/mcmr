package gr.tuc.softnet.engine;

import gr.tuc.softnet.core.ConfStringConstants;
import gr.tuc.softnet.core.StringConstants;
import gr.tuc.softnet.mapred.MCMapper;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by vagvaz on 03/03/16.
 */
public class TaskConfiguration extends HierarchicalConfiguration implements Serializable {


    public TaskConfiguration() {
        super();
    }
    public void setID(String id){
        setProperty(ConfStringConstants.TASK_ID, id);
    }
    public String getID() {
        return getString(ConfStringConstants.TASK_ID);
    }

    public String getNodeID() {
        return getString(ConfStringConstants.NODE_ID);
    }

    public String getCoordinator() {
        return getString(ConfStringConstants.COORDINATOR);
    }

    public void setJobID(String jobID){setProperty(ConfStringConstants.JOB_ID,jobID);}
    public String getJobID() {
        return getString(ConfStringConstants.JOB_ID);
    }

    public boolean isBatch() {
        return getString(ConfStringConstants.TASK_TYPE,StringConstants.BATCH_TASK).equals(StringConstants.BATCH_TASK);
    }

    public Class<? extends WritableComparable> getKeyClass() {
        return (Class) getProperty(ConfStringConstants.KEY_CLASS);
    }

    public void setKeyClass(Class keyClass) {
        setProperty(ConfStringConstants.KEY_CLASS, keyClass);
    }

    public Class<? extends Writable> getValueClass() {
        return (Class) getProperty(ConfStringConstants.VALUE_CLASS);
    }

    public void setValueClass(Class<? extends Writable> valueClass) {
        setProperty(ConfStringConstants.VALUE_CLASS, valueClass);
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
    public void setFederationReducerClass(Class<? extends Reducer<?,?,?,?>> federationReducerClass){
        setProperty(ConfStringConstants.FEDERATION_REDUCER_CLASS,federationReducerClass);
    }

    public Class<? extends Reducer<?, ?, ?, ?>> getLocalReducerClass() {
        return (Class<? extends Reducer<?, ?, ?, ?>>) getProperty(ConfStringConstants.LOCAL_REDUCER_CLASS);
    }

    public void setLocalReducerClass(Class<? extends Reducer<?,?,?,?>> reducerClass){
        setProperty(ConfStringConstants.LOCAL_REDUCER_CLASS,reducerClass);
    }


    public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() {
        return (Class) getProperty(ConfStringConstants.MAP_CLASS);
    }

    public void setMapperClass(Class<? extends MCMapper> mapperClass) {
        setProperty(ConfStringConstants.MAP_CLASS, mapperClass);
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

    public String getJar() {
        return getString(ConfStringConstants.JAR_NAME);
    }

    public void setJar(String jar) {
        setProperty(ConfStringConstants.JAR_NAME, jar);
    }

    public Class<? extends WritableComparable> getOutKeyClass() {
        return (Class<? extends WritableComparable>) getProperty(ConfStringConstants.OUT_KEY_CLASS);
    }

    public void setOutKeyClass(Class<? extends WritableComparable> outKeyClass) {
        setProperty(ConfStringConstants.OUT_KEY_CLASS, outKeyClass);
    }

    public Class<? extends Writable> getOutValueClass() {
        return (Class<? extends Writable>) getProperty(ConfStringConstants.OUT_VALUE_CLASS);
    }

    public void setOutValueClass(Class<? extends Writable> outValueClass) {
        setProperty(ConfStringConstants.OUT_VALUE_CLASS, outValueClass);
    }

    public boolean isMap() {
        return getBoolean(ConfStringConstants.IS_MAP_TASK, false);
    }

    public void setMap(boolean isMap) {
        setProperty(ConfStringConstants.IS_MAP_TASK, isMap);
    }

    public boolean isLocalReduce() {
        return getBoolean(ConfStringConstants.IS_LOCAL_REDUCE, false);
    }

    public void setLocalReduce(boolean isLocalReduce) {
        setProperty(ConfStringConstants.IS_LOCAL_REDUCE, isLocalReduce);
    }

    public boolean isFederationReduce() {
        return getBoolean(ConfStringConstants.IS_FEDERATION_REDUCE, false);
    }

    public void setFederationReduce(boolean isFederationReduce) {
        setProperty(ConfStringConstants.IS_FEDERATION_REDUCE, isFederationReduce);
    }

    public Class<?> getMapOutputKeyClass() {
        return (Class<?>) getProperty(ConfStringConstants.MAP_OUTPUT_KEY_CLASS);
    }

    public void setMapOuputKeyClass(Class<? extends WritableComparable> keyClass) {
        setProperty(ConfStringConstants.MAP_OUTPUT_KEY_CLASS, keyClass);
    }

    public void setMapOutputValueClass(Class<? extends Writable> classValue) {
        setProperty(ConfStringConstants.MAP_OUTPUT_VALUE_CLASS,classValue);
    }

    public void setTargetNode(String id) {
        setProperty(ConfStringConstants.NODE_ID,id);
    }
    public String getNodeTargetNode(){
        return getString(ConfStringConstants.NODE_ID);
    }

    public void setIsBatch(boolean isbatch) {
        if(isbatch) {
            setProperty(ConfStringConstants.TASK_TYPE,StringConstants.BATCH_TASK);
        }else{
            setProperty(ConfStringConstants.TASK_TYPE,StringConstants.PIPELINE_TASK);
        }
    }

    public void setTargetCloud(String cloud) {
        setProperty(ConfStringConstants.MICRO_CLOUD, cloud);
    }

    public String getTargetCloud(){
        return getString(ConfStringConstants.MICRO_CLOUD);
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

    public Map<String, Serializable> getJobConfiguration() {
        return (Map<String, Serializable>) getProperty(ConfStringConstants.USER_JOB_CONFIG);
    }
    public void setJobConfiguration(Map<String, Serializable> serializableMap){
        setProperty(ConfStringConstants.USER_JOB_CONFIG,serializableMap);
    }
}
