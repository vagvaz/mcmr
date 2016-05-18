package gr.tuc.softnet.engine;

import gr.tuc.softnet.core.NodeStatus;
import gr.tuc.softnet.netty.MCDataTransport;

/**
 * Created by vagvaz on 18/05/16.
 */
public class MCJobProxy {
  NodeStatus coordinator;
  JobConfiguration configuration;
  MCDataTransport transport;
  public MCJobProxy(NodeStatus node, JobConfiguration configuration, MCDataTransport dataTransport) {
    coordinator = node;
    this.configuration = configuration;
  }

  public JobStatus getJobStatus(){
    return transport.getJobStatus(coordinator.getID(),configuration.getJobID());
  }

  public void cancelJob(){
    transport.cancelJob(coordinator.getID(),configuration.getJobID(),configuration.getClouds());
  }
  public void waitForCompletion(){
    transport.waitForJobCompletion(coordinator.getID(),configuration.getJobID());
  }
}
