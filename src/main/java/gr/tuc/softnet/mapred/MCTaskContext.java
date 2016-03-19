package gr.tuc.softnet.mapred;

import gr.tuc.softnet.engine.TaskConfiguration;
import gr.tuc.softnet.kvs.KVSProxy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.security.Credentials;

import java.io.IOException;
import java.net.URI;

/**
 * Created by vagvaz on 10/03/16.
 */
public class MCTaskContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> implements  TaskInputOutputContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT>,MapContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT>,ReduceContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
    TaskConfiguration configuration;
    KVSProxy<KEYOUT,VALUEOUT> output;
    private Configuration hadoopConfiguration;



    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return false;
    }

    @Override
    public KEYIN getCurrentKey() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public VALUEIN getCurrentValue() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public void write(KEYOUT key, VALUEOUT value) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter() {
        return null;
    }

    @Override
    public TaskAttemptID getTaskAttemptID() {
        return null;
    }

    @Override
    public void setStatus(String msg) {

    }

    @Override
    public String getStatus() {
        return null;
    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public Counter getCounter(Enum<?> counterName) {
        return null;
    }

    @Override
    public Counter getCounter(String groupName, String counterName) {
        return null;
    }

    @Override
    public Configuration getConfiguration() {
        return hadoopConfiguration;
    }

    @Override
    public Credentials getCredentials() {
        return null;
    }

    @Override
    public JobID getJobID() {
        return null;
    }

    @Override
    public int getNumReduceTasks() {
        return 0;
    }

    @Override
    public Path getWorkingDirectory() throws IOException {
        return null;
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return null;
    }

    @Override
    public Class<?> getOutputValueClass() {
        return null;
    }

    @Override
    public Class<?> getMapOutputKeyClass() {
        return null;
    }

    @Override
    public Class<?> getMapOutputValueClass() {
        return null;
    }

    @Override
    public String getJobName() {
        return null;
    }

    @Override
    public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
        return null;
    }

    @Override
    public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
        return configuration.getMapClass();
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
        return configuration.getCombinerClass();
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
        return configuration.getFederationReducerClass();
    }


    public Class<? extends Reducer<?, ?, ?, ?>> getLocalReducerClass() throws ClassNotFoundException {
        return configuration.getLocalReducerClass();
    }

    public Class<? extends Reducer<?, ?, ?, ?>> getFedurationReducerClass() throws ClassNotFoundException {
        return getReducerClass();
    }
    @Override
    public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
        return null;
    }

    @Override
    public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
        return configuration.getPartitionerClass();
    }

    @Override
    public RawComparator<?> getSortComparator() {
        return null;
    }

    @Override
    public String getJar() {
        return configuration.getJar();
    }

    @Override
    public RawComparator<?> getCombinerKeyGroupingComparator() {
        return null;
    }

    @Override
    public RawComparator<?> getGroupingComparator() {
        return null;
    }

    @Override
    public boolean getJobSetupCleanupNeeded() {
        return false;
    }

    @Override
    public boolean getTaskCleanupNeeded() {
        return false;
    }

    @Override
    public boolean getProfileEnabled() {
        return false;
    }

    @Override
    public String getProfileParams() {
        return null;
    }

    @Override
    public Configuration.IntegerRanges getProfileTaskRange(boolean isMap) {
        return null;
    }

    @Override
    public String getUser() {
        return null;
    }

    @Override
    public boolean getSymlink() {
        return false;
    }

    @Override
    public Path[] getArchiveClassPaths() {
        return new Path[0];
    }

    @Override
    public URI[] getCacheArchives() throws IOException {
        return new URI[0];
    }

    @Override
    public URI[] getCacheFiles() throws IOException {
        return new URI[0];
    }

    @Override
    public Path[] getLocalCacheArchives() throws IOException {
        return new Path[0];
    }

    @Override
    public Path[] getLocalCacheFiles() throws IOException {
        return new Path[0];
    }

    @Override
    public Path[] getFileClassPaths() {
        return new Path[0];
    }

    @Override
    public String[] getArchiveTimestamps() {
        return new String[0];
    }

    @Override
    public String[] getFileTimestamps() {
        return new String[0];
    }

    @Override
    public int getMaxMapAttempts() {
        return 0;
    }

    @Override
    public int getMaxReduceAttempts() {
        return 0;
    }

    @Override
    public void progress() {

    }

    public boolean isPipelined(){
        return false;
    }

    public boolean isReduceLocal(){
        return false;
    }

    public boolean isMap(){
        return false;
    }

    @Override
    public InputSplit getInputSplit() {
        return null;
    }

    @Override
    public boolean nextKey() throws IOException, InterruptedException {
        return false;
    }

    @Override
    public Iterable<VALUEIN> getValues() throws IOException, InterruptedException {
        return null;
    }
}
