package gr.tuc.softnet.core;

import gr.tuc.softnet.engine.JobConfiguration;
import gr.tuc.softnet.engine.MCJobProxy;
import gr.tuc.softnet.netty.MCDataTransport;
import org.apache.hadoop.mapred.JobConf;

import java.util.UUID;

/**
 * Created by vagvaz on 18/05/16.
 */
public class JobSubmitter {
  public static MCJobProxy submitJob(JobConfiguration configuration){
    MCDataTransport transport = InjectorUtils.getInjector().getInstance(MCDataTransport.class);
    String jobID = UUID.randomUUID().toString();
    configuration.setJobID(jobID);
    return transport.submitJob(configuration);
  }
  public static MCJobProxy submitJob(JobConfiguration configuration, String microcloud){
    MCDataTransport transport = InjectorUtils.getInjector().getInstance(MCDataTransport.class);
    String jobID = UUID.randomUUID().toString();
    configuration.setJobID(jobID);
    return transport.submitJob(configuration,microcloud);
  }
}
