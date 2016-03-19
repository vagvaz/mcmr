package gr.tuc.softnet.engine;

import gr.tuc.softnet.core.MCInitializable;
import gr.tuc.softnet.core.NodeStatus;
import org.apache.commons.configuration.Configuration;

import java.util.List;

/**
 * Created by vagvaz on 10/02/16.
 */
public interface JobManager  extends MCInitializable,IDable{
    boolean startJob(List<NodeStatus> nodes, JobConfiguration jobConfiguration);
    boolean startJob(JobConfiguration jobConfiguration);
    boolean cancelJob(List<NodeStatus> nodes, String jobID);
    boolean cancelJob(String jobID);
    void completedJob(String jobID);
    JobStatus getJobStatus(String jobID);
    void waitForCompletion(String jobID);
    void taskCompleted(String jobID, String id);
}
