package gr.tuc.softnet.netty;

import gr.tuc.softnet.core.InjectorUtils;
import gr.tuc.softnet.core.NodeManager;
import gr.tuc.softnet.engine.JobManager;
import gr.tuc.softnet.engine.TaskManager;
import gr.tuc.softnet.kvs.KVSManager;
import gr.tuc.softnet.netty.messages.MCMessageWrapper;
import io.netty.channel.Channel;

/**
 * Created by vagvaz on 18/05/16.
 */
public abstract class MCMessageHandler {
  protected MCDataTransport transport;
  protected KVSManager kvsManager;
  protected NodeManager nodeManager;
  protected JobManager jobManager;
  protected TaskManager taskManager;
  public MCMessageHandler(){
    transport = InjectorUtils.getInjector().getInstance(MCDataTransport.class);
    kvsManager = InjectorUtils.getInjector().getInstance(KVSManager.class);
    nodeManager = InjectorUtils.getInjector().getInstance(NodeManager.class);
    jobManager = InjectorUtils.getInjector().getInstance(JobManager.class);
    taskManager = InjectorUtils.getInjector().getInstance(TaskManager.class);
  }
  abstract public MCMessageWrapper process(Channel node, MCMessageWrapper wrapper);
}
