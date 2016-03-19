package gr.tuc.softnet.engine;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import gr.tuc.softnet.core.ConfStringConstants;
import gr.tuc.softnet.core.MCConfiguration;
import gr.tuc.softnet.core.NodeManager;
import gr.tuc.softnet.core.NodeStatus;
import gr.tuc.softnet.netty.MCDataTransport;
import org.apache.commons.collections.map.HashedMap;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Created by vagvaz on 03/03/16.
 */
@Singleton
public class JobManagerImpl implements JobManager {
    @Inject
    MCDataTransport dataTransport;
    @Inject
    TaskManager taskManager;
    @Inject
    MCConfiguration configuration;
    @Inject
    NodeManager nodeManager;
    Map<String,MCJob> jobInfo;
    Logger logger = org.slf4j.LoggerFactory.getLogger(JobManager.class);
    @Override
    public boolean startJob(List<NodeStatus> nodes, JobConfiguration jobConfiguration) {
        MCJob job = JobUtils.getJob(jobConfiguration);
        job.setNodes(nodes);
        return boostrapJob(job);
    }

    @Override
    public boolean startJob(JobConfiguration jobConfiguration) {
        MCJob job = JobUtils.getJob(jobConfiguration);
        jobInfo.put(job.getID(),job);
        return boostrapJob(job);
    }

    private boolean boostrapJob(MCJob job) {
        List<TaskConfiguration> taskConfigurations = job.getTaskConfigurations();
        for(TaskConfiguration task : taskConfigurations){

            if(task.getNodeID().equals(configuration.getURI())){
                taskManager.startTask(task);
            }
            dataTransport.startTask(task);
        }
        return true;
    }

    @Override
    public boolean cancelJob(List<NodeStatus> nodes, String jobID) {
        MCJob job = jobInfo.get(jobID);
        if(job == null){
            logger.error("Job: " + jobID + " is not handled bu this JobManager");
            return dataTransport.cancelJob(nodes,jobID);
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
            return true;
        }else{
            for(Map.Entry<NodeStatus,TaskConfiguration> nodeTask: job.getTasks()){
                if(nodeTask.getKey().getID().equals(getID())){
                    taskManager.cancelTask(nodeTask.getValue().getID());
                }else{
                    dataTransport.cancelTask(nodeTask.getKey().getID(),nodeTask.getValue().getID());
                }
            }
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
            return dataTransport.getJobStatus(jobID);
        }else{
            return job.getStatus();
        }
    }

    @Override
    public void waitForCompletion(String jobID) {
        MCJob job = jobInfo.get(jobID);
        if(job == null) {
            logger.error("waitForCompletion called but " + jobID + " is not handled by " + getID());
        }
        job.waitForCompletion();
    }

    @Override
    public void taskCompleted(String jobID, String id) {

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
    }
}
