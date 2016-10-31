package gr.tuc.softnet.engine;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import gr.tuc.softnet.core.ConfStringConstants;
import gr.tuc.softnet.core.MCConfiguration;
import gr.tuc.softnet.core.NodeManager;
import gr.tuc.softnet.core.NodeStatus;
import gr.tuc.softnet.kvs.KVSConfiguration;
import gr.tuc.softnet.kvs.KVSManager;
import gr.tuc.softnet.netty.MCDataTransport;
import gr.tuc.softnet.netty.messages.NoMoreInputMessage;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import rx.Observable;
import rx.Subscriber;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by vagvaz on 03/03/16.
 */
@Singleton
public class JobManagerImpl implements JobManager, Observable.OnSubscribe<JobConfiguration> {
    @Inject
    MCDataTransport dataTransport;
    @Inject
    TaskManager taskManager;
    @Inject
    MCConfiguration configuration;
    @Inject
    NodeManager nodeManager;
    @Inject
    KVSManager kvsManager;
    Map<String,MCJob> jobInfo;
    Map<String,JobConfiguration> remoteJobs;
    Map<String,List<Object>> pendingRequests;
    Logger logger = org.slf4j.LoggerFactory.getLogger(JobManager.class);
    volatile ReentrantReadWriteLock mutex= new ReentrantReadWriteLock(true);
    private List<Subscriber<? super JobConfiguration>> subscribers;

    @Override
    public boolean startJob(List<NodeStatus> nodes, JobConfiguration jobConfiguration) {
        MCJob job = JobUtils.getJob(jobConfiguration);
        job.setNodes(nodes);
        return bootstrapJob(job);
    }

    @Override
    public boolean startJob(JobConfiguration jobConfiguration) {
        MCJob job = JobUtils.getJob(jobConfiguration);
        jobInfo.put(job.getID(),job);
        return bootstrapJob(job);
    }

    private boolean bootstrapJob(MCJob job) {
        KVSConfiguration kvsConfiguration = job.getMapOuputConfiguration();
        kvsManager.createKVS(kvsConfiguration.getName(),kvsConfiguration);
        if(job.getJobConfiguration().hasLocalReduce()){
            kvsConfiguration = job.getLocalReduceOutputConfiguration();
            kvsManager.createKVS(kvsConfiguration.getName(),kvsConfiguration);
        }
        kvsConfiguration =  job.getFederationReduceOutput();
        kvsManager.createKVS(kvsConfiguration.getName(),kvsConfiguration);

       runReadyTasks(job);

        return true;
    }

    private void runReadyTasks(MCJob job) {
        List<TaskConfiguration> taskConfigurations = job.getReadyToRunTaskConfigurations();

        for(int index = taskConfigurations.size()-1; index >= 0; index--){
            TaskConfiguration task  = taskConfigurations.get(index);
            if(task.getNodeID().equals(configuration.getURI())){
                taskManager.startTask(task);
            }else {
                dataTransport.startTask(task);
            }
        }
    }

    @Override
    public boolean cancelJob(List<NodeStatus> nodes, String jobID) {
        MCJob job = jobInfo.get(jobID);
        if(job == null){
            logger.error("Job: " + jobID + " is not handled bu this JobManager");
            return dataTransport.cancelJob(jobID, jobID,new LinkedList<>());
        }
        else{
            cancelJob(jobID);
            return true;
        }
    }

    @Override
    public boolean cancelJob(String jobID) {
        boolean result = false;
        MCJob job = jobInfo.get(jobID);
        if(job == null){
            logger.error("Job: " + jobID + "cannot be canceled cause is not handle by " + getID());
            return false;
        }else{
            for(Map.Entry<NodeStatus,TaskConfiguration> nodeTask: job.getTasks()){
                if(nodeTask.getKey().getID().equals(getID())){
                    taskManager.cancelTask(nodeTask.getValue().getID());
                }else{
                    dataTransport.cancelTask(nodeTask.getKey().getID(),nodeTask.getValue().getID());
                }
            }
            result = true;
        }
        return result;
    }

    @Override
    public void completedJob(JobConfiguration job) {
        //Nothing to do for now no remote monitoring of a job
        List<Object> mutexes = this.pendingRequests.get(job.getJobID());
        this.remoteJobs.put(job.getJobID(), job);
        if(mutexes != null){
            for(Object mutex : mutexes){
                synchronized (mutex) {
                    mutex.notify();
                }
            }
        }
    }

    @Override
    public JobStatus getJobStatus(String jobID) {
        MCJob job = jobInfo.get(jobID);
        if(job == null){
            return dataTransport.getJobStatus(job.getJobConfiguration().getJobID(), jobID);
        }else{
            return job.getStatus();
        }
    }

    @Override
    public void waitForCompletion(String jobID) {
        MCJob job = jobInfo.get(jobID);
        if(job == null) {
            logger.error("waitForCompletion called but " + jobID + " is not handled by " + getID());
            return;
        }
        job.waitForCompletion();
    }

    @Override
    public void taskCompleted(String jobID, String cloud, String id) {
        MCJob job = this.jobInfo.get(jobID);
        if(job == null){
            logger.error("Unknown job " + jobID + " task completed received");
            return;
        }
        Map<String,NoMoreInputMessage> noMoreInputMessageMap = job.completeTask(cloud,id);
        for(Map.Entry<String,NoMoreInputMessage> entry : noMoreInputMessageMap.entrySet() ){
            dataTransport.sendAndFlush(entry.getKey(),entry.getValue());
        }

        runReadyTasks(job);
        if(job.isCompleted()){
            for(Subscriber<? super JobConfiguration> subscriber : subscribers){
                subscriber.onNext(job.getJobConfiguration());
                subscriber.onCompleted();
            }
            dataTransport.jobCompleted(job.getJobConfiguration());
        }
    }
    @Override
    public void addRemoteJob(JobConfiguration configuration){
        synchronized (this.mutex){
            if(!jobInfo.containsKey(configuration.getJobID())) {
                this.remoteJobs.put(configuration.getJobID(), configuration);
            }
        }
    }
    @Override public void waitForJobCompletion(String jobID) {
        MCJob job = this.jobInfo.get(jobID);
        if(job == null){
            JobConfiguration configuration = this.remoteJobs.get(jobID);
            if (configuration == null){
                return;
            } else{
                Lock lock = this.mutex.writeLock();
                lock.lock();
                {
                    Object new_mutex = new Object();
                    List<Object> mutexes = this.pendingRequests.get(jobID);
                    if (mutexes == null){
                        mutexes = new LinkedList<>();
                        this.pendingRequests.put(jobID,mutexes);
                    }
                    mutexes.add(new_mutex);
                    synchronized (new_mutex){
                        dataTransport.waitForJobCompletion(
                          (String) configuration.getProperty(ConfStringConstants.COORDINATOR), jobID);
                        lock.unlock();
                        try {
                            new_mutex.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                }
            }
        }else{
            if(job.isCompleted()){
                return;
            }
            Lock lock = this.mutex.writeLock();
            lock.lock();
            {
                Object new_mutex = new Object();
                List<Object> mutexes = this.pendingRequests.get(jobID);
                if (mutexes == null){
                    mutexes = new LinkedList<>();
                }
                mutexes.add(new_mutex);
                synchronized (new_mutex){
                    lock.unlock();
                    try {
                        new_mutex.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    @Override
    public String getID() {
        return configuration.conf().getString(ConfStringConstants.JOB_MANAGER_ID,nodeManager.getID()+".jobManager");
    }

    @Override
    public MCConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public void initialize() {
        jobInfo = new HashedMap();
        pendingRequests = new HashedMap();
        remoteJobs = new HashedMap();
        subscribers = new ArrayList<>();
    }

    @Override public void call(Subscriber<? super JobConfiguration> subscriber) {
        subscribers.add(subscriber);
    }
}
