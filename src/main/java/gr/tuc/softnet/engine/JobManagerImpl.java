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
import org.slf4j.Logger;
import rx.Observable;
import rx.Subscriber;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by vagvaz on 03/03/16.
 */
@Singleton
public class JobManagerImpl implements JobManager, Observable.OnSubscribe<String> {
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
    Logger logger = org.slf4j.LoggerFactory.getLogger(JobManager.class);
    private List<Subscriber<? super String>> subscribers;

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

        for(TaskConfiguration task : taskConfigurations){

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
            return dataTransport.cancelJob(coordinator.getID(), nodes,jobID);
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
    public void completedJob(String jobID) {
        //Nothing to do for now no remote monitoring of a job
    }

    @Override
    public JobStatus getJobStatus(String jobID) {
        MCJob job = jobInfo.get(jobID);
        if(job == null){
            return dataTransport.getJobStatus(coordinator.getID(), jobID);
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
            dataTransport.send(entry.getKey(),entry.getValue());
        }

        runReadyTasks(job);
        if(job.isCompleted()){
            for(Subscriber<? super String> subscriber : subscribers){
                subscriber.onNext(job.getID());
                subscriber.onCompleted();
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
        subscribers = new ArrayList<>();
    }

    @Override public void call(Subscriber<? super String> subscriber) {
        subscribers.add(subscriber);
    }
}
