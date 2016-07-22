package gr.tuc.softnet.engine;

import gr.tuc.softnet.core.ConfStringConstants;
import gr.tuc.softnet.core.NodeStatus;
import gr.tuc.softnet.netty.MCDataTransport;

/**
 * Created by vagvaz on 18/05/16.
 */
public class MCJobProxy {
  NodeStatus coordinator;
  JobConfiguration configuration;
  JobManager jobManager;
  public MCJobProxy(NodeStatus node, JobConfiguration configuration, JobManager jobManager) {
    coordinator = node;
    this.configuration = configuration;
    this.jobManager = jobManager;
    this.configuration.setProperty(ConfStringConstants.COORDINATOR,node.getID());
    jobManager.addRemoteJob(this.configuration);
  }

  public JobStatus getJobStatus(){
    return jobManager.getJobStatus(configuration.getJobID());
  }

  public void cancelJob(){
    jobManager.cancelJob(configuration.getJobID());
  }
  public void waitForCompletion(){
    jobManager.waitForJobCompletion(configuration.getJobID());
  }
}
