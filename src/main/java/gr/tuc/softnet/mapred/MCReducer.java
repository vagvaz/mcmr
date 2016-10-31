package gr.tuc.softnet.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.security.Credentials;

import java.io.IOException;
import java.net.URI;

/**
 * Created by vagvaz on 08/03/16.
 */
public abstract class MCReducer<KEYIN extends WritableComparable, VALUEIN extends Writable,KEYOUT extends WritableComparable,VALUEOUT extends Writable> extends Reducer<KEYIN, VALUEIN,KEYOUT,VALUEOUT> {
    public Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context
    getReducerContext(ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reduceContext) {
        return new Context(reduceContext);
    }

    @Override
    protected void setup(Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(KEYIN key, Iterable<VALUEIN> values, Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }

    @Override
    protected void cleanup(Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }


    public class Context
            extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context {

        public MCTaskContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> getReduceContext() {
            return reduceContext;
        }

        protected MCTaskContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reduceContext;

        public Context(ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reduceContext)
        {
            this.reduceContext = (MCTaskContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT>) reduceContext;
        }

        public boolean isLocalReduce(){
            return this.reduceContext.isLocalReduce();
        }

        public boolean isPipelined(){
            return this.reduceContext.isPipelined();
        }

        @Override
        public KEYIN getCurrentKey() throws IOException, InterruptedException {
            return reduceContext.getCurrentKey();
        }

        @Override
        public VALUEIN getCurrentValue() throws IOException, InterruptedException {
            return reduceContext.getCurrentValue();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return reduceContext.nextKeyValue();
        }

        @Override
        public Counter getCounter(Enum counterName) {
            return reduceContext.getCounter(counterName);
        }

        @Override
        public Counter getCounter(String groupName, String counterName) {
            return reduceContext.getCounter(groupName, counterName);
        }

        @Override
        public OutputCommitter getOutputCommitter() {
            return reduceContext.getOutputCommitter();
        }

        @Override
        public void write(KEYOUT key, VALUEOUT value) throws IOException,
                InterruptedException {
            reduceContext.write(key, value);
        }

        @Override
        public String getStatus() {
            return reduceContext.getStatus();
        }

        @Override
        public TaskAttemptID getTaskAttemptID() {
            return reduceContext.getTaskAttemptID();
        }

        @Override
        public void setStatus(String msg) {
            reduceContext.setStatus(msg);
        }

        @Override
        public Path[] getArchiveClassPaths() {
            return reduceContext.getArchiveClassPaths();
        }

        @Override
        public String[] getArchiveTimestamps() {
            return reduceContext.getArchiveTimestamps();
        }

        @Override
        public URI[] getCacheArchives() throws IOException {
            return reduceContext.getCacheArchives();
        }

        @Override
        public URI[] getCacheFiles() throws IOException {
            return reduceContext.getCacheFiles();
        }

        @Override
        public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
                throws ClassNotFoundException {
            return reduceContext.getCombinerClass();
        }

        @Override
        public Configuration getConfiguration() {
            return reduceContext.getConfiguration();
        }

        @Override
        public Path[] getFileClassPaths() {
            return reduceContext.getFileClassPaths();
        }

        @Override
        public String[] getFileTimestamps() {
            return reduceContext.getFileTimestamps();
        }

        @Override
        public RawComparator<?> getCombinerKeyGroupingComparator() {
            return reduceContext.getCombinerKeyGroupingComparator();
        }

        @Override
        public RawComparator<?> getGroupingComparator() {
            return reduceContext.getGroupingComparator();
        }

        @Override
        public Class<? extends InputFormat<?, ?>> getInputFormatClass()
                throws ClassNotFoundException {
            return reduceContext.getInputFormatClass();
        }

        @Override
        public String getJar() {
            return reduceContext.getJar();
        }

        @Override
        public JobID getJobID() {
            return reduceContext.getJobID();
        }

        @Override
        public String getJobName() {
            return reduceContext.getJobName();
        }

        @Override
        public boolean getJobSetupCleanupNeeded() {
            return reduceContext.getJobSetupCleanupNeeded();
        }

        @Override
        public boolean getTaskCleanupNeeded() {
            return reduceContext.getTaskCleanupNeeded();
        }

        @Override
        public Path[] getLocalCacheArchives() throws IOException {
            return reduceContext.getLocalCacheArchives();
        }

        @Override
        public Path[] getLocalCacheFiles() throws IOException {
            return reduceContext.getLocalCacheFiles();
        }

        @Override
        public Class<?> getMapOutputKeyClass() {
            return reduceContext.getMapOutputKeyClass();
        }

        @Override
        public Class<?> getMapOutputValueClass() {
            return reduceContext.getMapOutputValueClass();
        }

        @Override
        public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
                throws ClassNotFoundException {
            return reduceContext.getMapperClass();
        }

        @Override
        public int getMaxMapAttempts() {
            return reduceContext.getMaxMapAttempts();
        }

        @Override
        public int getMaxReduceAttempts() {
            return reduceContext.getMaxReduceAttempts();
        }

        @Override
        public int getNumReduceTasks() {
            return reduceContext.getNumReduceTasks();
        }

        @Override
        public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
                throws ClassNotFoundException {
            return reduceContext.getOutputFormatClass();
        }

        @Override
        public Class<?> getOutputKeyClass() {
            return reduceContext.getOutputKeyClass();
        }

        @Override
        public Class<?> getOutputValueClass() {
            return reduceContext.getOutputValueClass();
        }

        @Override
        public Class<? extends Partitioner<?, ?>> getPartitionerClass()
                throws ClassNotFoundException {
            return reduceContext.getPartitionerClass();
        }

        @Override
        public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
                throws ClassNotFoundException {
            return reduceContext.getReducerClass();
        }

        @Override
        public RawComparator<?> getSortComparator() {
            return reduceContext.getSortComparator();
        }

        @Override
        public boolean getSymlink() {
            return reduceContext.getSymlink();
        }

        @Override
        public Path getWorkingDirectory() throws IOException {
            return reduceContext.getWorkingDirectory();
        }

        @Override
        public void progress() {
            reduceContext.progress();
        }

        @Override
        public Iterable<VALUEIN> getValues() throws IOException,
                InterruptedException {
            return reduceContext.getValues();
        }

        @Override
        public boolean nextKey() throws IOException, InterruptedException {
            return reduceContext.nextKey();
        }

        @Override
        public boolean getProfileEnabled() {
            return reduceContext.getProfileEnabled();
        }

        @Override
        public String getProfileParams() {
            return reduceContext.getProfileParams();
        }

        @Override
        public Configuration.IntegerRanges getProfileTaskRange(boolean isMap) {
            return reduceContext.getProfileTaskRange(isMap);
        }

        @Override
        public String getUser() {
            return reduceContext.getUser();
        }

        @Override
        public Credentials getCredentials() {
            return reduceContext.getCredentials();
        }

        @Override
        public float getProgress() {
            return reduceContext.getProgress();
        }
    }
}
