package gr.tuc.softnet.engine;

import com.google.inject.Inject;
import gr.tuc.softnet.core.ConfStringConstants;
import gr.tuc.softnet.core.MCConfiguration;
import gr.tuc.softnet.core.NodeManager;
import gr.tuc.softnet.core.NodeStatus;
import gr.tuc.softnet.kvs.KVSConfiguration;
import gr.tuc.softnet.netty.messages.NoMoreInputMessage;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by vagvaz on 06/04/16.
 */
enum Stage{
  INIT,MAP,REDUCE
}
public class MCJobImpl implements MCJob {
  @Inject
  NodeManager nodeManager;
  JobConfiguration jobConf;
  List<NodeStatus> nodes;
  Map<String,Map<String,NodeStatus>> mc_nodes;
  Map<String,Map<String,TaskConfiguration>> mapTasks;
  Map<String,Map<String,TaskConfiguration>> runningMapTasks;
  Map<String,Map<String,TaskConfiguration>> completedMapTasks;
  Map<String,Map<String,TaskConfiguration>> runningLocalReduceTasks;
  Map<String,Map<String,TaskConfiguration>> localReduceTasks;
  Map<String,Map<String,TaskConfiguration>> completedLocalReduceTasks;
  Map<String,Map<String,TaskConfiguration>> runningFederationReduceTasks;
  Map<String,Map<String,TaskConfiguration>> federationReduceTasks;
  Map<String,Map<String,TaskConfiguration>> completedFederationReduceTasks;
  private KVSConfiguration mapOutputKVSConf;
  private KVSConfiguration localReduceOutputKVSConf;
  private KVSConfiguration federationReduceOutputKVSConf;
  private boolean isCompleted = false;
  private JobStatus jobStatus;
  private Stage currentStage = Stage.INIT;
  private Logger logger = LoggerFactory.getLogger(MCJobImpl.class);

  @Override public void setNodes(List<NodeStatus> nodes) {
    this.nodes = nodes;
  }

  @Override public void initialize(JobConfiguration jobConf, Map<String, NodeStatus> clouds) {
    jobStatus = new JobStatus(this);
    mapTasks = new HashedMap();
    runningMapTasks = new HashedMap();
    completedMapTasks = new HashedMap();
    localReduceTasks = new HashedMap();
    runningLocalReduceTasks = new HashedMap();
    completedLocalReduceTasks =  new HashedMap();
    federationReduceTasks = new HashedMap();
    runningFederationReduceTasks = new HashedMap();
    completedFederationReduceTasks = new HashedMap();

    mapOutputKVSConf = initMapOutput();
    localReduceOutputKVSConf = initLocalReduceOutput();
    federationReduceOutputKVSConf = initFederationReduceOutput();
    if(nodes != null){
      for(NodeStatus node : nodes){
        Map<String,NodeStatus> cloudNodes = mc_nodes.get(node.getMicrocloud());
        if(cloudNodes == null) {
          cloudNodes = new HashedMap();
          mc_nodes.put(node.getMicrocloud(),cloudNodes);
        }
        cloudNodes.put(node.getID(),node);
        }
    }
    else{
      for(String mc : jobConf.getClouds()){
        mc_nodes.put(mc,nodeManager.getMicrocloudInfo(mc));
      }
    }

    for(Map.Entry<String, Map<String, NodeStatus>> entry : mc_nodes.entrySet()){
      Map<String,TaskConfiguration> tasks = new HashedMap();
      for(Map.Entry<String,NodeStatus> nodeEntry : entry.getValue().entrySet()){
        TaskConfiguration task = createMapTask(nodeEntry.getValue());
        task.setTargetCloud(entry.getKey());
        tasks.put(task.getID(),task);
      }
      mapTasks.put(entry.getKey(),tasks);
    }

    for(Map.Entry<String, Map<String, NodeStatus>> entry : mc_nodes.entrySet()){
      Map<String,TaskConfiguration> tasks = new HashedMap();
      for(Map.Entry<String,NodeStatus> nodeEntry : entry.getValue().entrySet()){
        TaskConfiguration task = createlocalReduceTask(nodeEntry.getValue());
        task.setTargetCloud(entry.getKey());
        tasks.put(task.getID(),task);
      }
      localReduceTasks.put(entry.getKey(),tasks);
    }

    for(Map.Entry<String, Map<String, NodeStatus>> entry : mc_nodes.entrySet()){
      Map<String,TaskConfiguration> tasks = new HashedMap();
      for(Map.Entry<String,NodeStatus> nodeEntry : entry.getValue().entrySet()){
        TaskConfiguration task = createFederationReduceTask(nodeEntry.getValue());
        task.setTargetCloud(entry.getKey());
        tasks.put(task.getID(),task);
      }
      federationReduceTasks.put(entry.getKey(),tasks);
    }
  }

  private TaskConfiguration createFederationReduceTask(NodeStatus value) {
    TaskConfiguration result = new TaskConfiguration();
    result.setFederationReduce(true);
    if(jobConf.hasLocalReduce()) {
      result.setInput(localReduceOutputKVSConf.getName());
    }else{
      result.setInput(mapOutputKVSConf.getName());
    }
    result.setOutput(federationReduceOutputKVSConf.getName());
    result.setProperty(ConfStringConstants.COORDINATOR,nodeManager.getNodeID());
    result.setJar(jobConf.getJar());
    result.setJobID(jobConf.getJobID());
    result.setOutKeyClass(jobConf.getFederationReduceOutputKeyClass());
    result.setOutValueClass(jobConf.getFederationReduceOutputValueClass());
    result.setID(jobConf.getJobID()+"-federation-reduce-task-"+value.getID());
    result.setPartitionerClass(jobConf.getPartitionerClass());
    result.setTargetNode(value.getID());
    result.setFederationReducerClass(jobConf.getFederationReducerClass());
    result.setIsBatch(!jobConf.isFederationReducePipeline());
    return  result;
  }

  private TaskConfiguration createlocalReduceTask(NodeStatus value) {
    if(!jobConf.hasLocalReduce()){
      return null;
    }
    TaskConfiguration result = new TaskConfiguration();
    result.setLocalReduce(true);
    result.setInput(mapOutputKVSConf.getName());
    result.setOutput(localReduceOutputKVSConf.getName());
    result.setProperty(ConfStringConstants.COORDINATOR,nodeManager.getNodeID());
    result.setJar(jobConf.getJar());
    result.setJobID(jobConf.getJobID());
    result.setOutKeyClass(jobConf.getLocalReduceOutputKeyClass());
    result.setOutValueClass(jobConf.getLocalReduceOutputValueClass());
    result.setID(jobConf.getJobID()+"-local-reduce-task-"+value.getID());
    result.setPartitionerClass(jobConf.getPartitionerClass());
    result.setTargetNode(value.getID());
    result.setLocalReducerClass(jobConf.getLocalReducerClass());
    result.setIsBatch(!jobConf.isLocalReducePipeline());
    return  result;
  }


  private TaskConfiguration createMapTask(NodeStatus value) {
    TaskConfiguration result = new TaskConfiguration();
    result.setMap(true);
    result.setInput(jobConf.getInput());
    result.setMapOuputKeyClass(jobConf.getMapOutputKeyClass());
    result.setMapOutputValueClass(jobConf.getMapOutputValueClass());
    result.setOutput(mapOutputKVSConf.getName());
    result.setCombinerClass(jobConf.getCombinerClass());
    result.setProperty(ConfStringConstants.COORDINATOR,nodeManager.getNodeID());
    result.setJar(jobConf.getJar());
    result.setJobID(jobConf.getJobID());
    result.setOutKeyClass(jobConf.getMapOutputKeyClass());
    result.setOutValueClass(jobConf.getMapOutputValueClass());
    result.setID(jobConf.getJobID()+"-map-task-"+value.getID());
    result.setPartitionerClass(jobConf.getPartitionerClass());
    result.setTargetNode(value.getID());
    result.setMapperClass(jobConf.getMapperClass());
    result.setIsBatch(!jobConf.isMapPipeline());
    return result;
  }

  private KVSConfiguration initLocalReduceOutput() {
    if(jobConf.hasLocalReduce()){
      return null;
    }
    KVSConfiguration result = new KVSConfiguration();
    result.setKeyClass(jobConf.getLocalReduceOutputKeyClass());
    result.setValueClass(jobConf.getLocalReduceOutputValueClass());
    result.setName(jobConf.getJobID()+".federation_in");
    result.setMaterialized(jobConf.isFederationReducePipeline());
    result.setLocal(false);
    return result;
  }

  private KVSConfiguration initMapOutput() {

    KVSConfiguration result = new KVSConfiguration();
    result.setKeyClass(jobConf.getMapOutputKeyClass());
    result.setValueClass(jobConf.getMapOutputValueClass());
    if(jobConf.hasLocalReduce()) {
      result.setName(jobConf.getJobID() + ".local_reduce_in");
    }else{
      result.setName(jobConf.getJobID() + ".federation_in");
    }
    result.setMaterialized(jobConf.isLocalReducePipeline());
    result.setLocal(false);
    return result;
  }

  private KVSConfiguration initFederationReduceOutput() {
    KVSConfiguration result = new KVSConfiguration();
    result.setKeyClass(jobConf.getFederationReduceOutputKeyClass());
    result.setValueClass(jobConf.getFederationReduceOutputValueClass());
    result.setName(jobConf.getOutput());
    result.setMaterialized(true);
    result.setLocal(false);
    return result;
  }

  @Override public List<? extends NodeStatus> getNodes() {
    return nodes;
  }

  @Override public void addTask(NodeStatus node, TaskConfiguration taskConfiguration) {
    throw new NotImplementedException("addTask");
  }

  @Override public Iterable<Map.Entry<NodeStatus, TaskConfiguration>> getTasks() {
    return null;
  }

  @Override public JobStatus getStatus() {
    return jobStatus;
  }

  @Override public void waitForCompletion() {

  }

  @Override public KVSConfiguration getMapOuputConfiguration() {
    return mapOutputKVSConf;
  }

  @Override public JobConfiguration getJobConfiguration() {
    return jobConf;
  }

  @Override public KVSConfiguration getLocalReduceOutputConfiguration() {
    return localReduceOutputKVSConf;
  }

  @Override public KVSConfiguration getFederationReduceOutput() {
    return federationReduceOutputKVSConf;
  }

  public List<TaskConfiguration> invokeMapTasks(String microcloud){
    List<TaskConfiguration> result = new LinkedList<>();
    for(Map.Entry<String,TaskConfiguration> taskEntry : mapTasks.get(microcloud).entrySet()){
      result.add(taskEntry.getValue());
    }
    //put batch into running mapTasks
    runningMapTasks.put(microcloud,mapTasks.get(microcloud));
    return result;
  }

  public List<TaskConfiguration> invokelocalReduceTasks(String microcloud){
    List<TaskConfiguration> result = new LinkedList<>();
    for(Map.Entry<String,TaskConfiguration> taskEntry : localReduceTasks.get(microcloud).entrySet()){
      result.add(taskEntry.getValue());
    }
    //put batch into running mapTasks
    runningLocalReduceTasks.put(microcloud,localReduceTasks.get(microcloud));
    return result;
  }

  public List<TaskConfiguration> invokeFederationReduceTasks(String microcloud){
    List<TaskConfiguration> result = new LinkedList<>();
    for(Map.Entry<String,TaskConfiguration> taskEntry : localReduceTasks.get(microcloud).entrySet()){
      result.add(taskEntry.getValue());
    }
    //put batch into running mapTasks
    runningLocalReduceTasks.put(microcloud,localReduceTasks.get(microcloud));
    return result;
  }

  @Override public synchronized List<TaskConfiguration> getReadyToRunTaskConfigurations() {
    List<TaskConfiguration> result = new LinkedList<>();
    switch (currentStage) {
      case INIT:
        for(Map.Entry<String,Map<String,TaskConfiguration>> entry : mapTasks.entrySet()){
          result.addAll(invokeMapTasks(entry.getKey()));
        }
        mapTasks.clear();
        if(jobConf.isLocalReducePipeline()){
          for(Map.Entry<String,Map<String,TaskConfiguration>> entry : localReduceTasks.entrySet()){
            result.addAll(invokelocalReduceTasks(entry.getKey()));
          }
          runningLocalReduceTasks.clear();
        }

        if(jobConf.isLocalReducePipeline()){
          for(Map.Entry<String,Map<String,TaskConfiguration>> entry : localReduceTasks.entrySet()) {
            result.addAll(invokeFederationReduceTasks(entry.getKey()));
          }
          runningFederationReduceTasks.clear();
        }

        break;
      case MAP:
        if(mapTasks.size() != 0){
          logger.error("MapTasks have not been invoked even though we are in the MAP Stage");
        }
        if(runningMapTasks.size() != 0){
          boolean allMapAreFinished = true;
          Iterator<Map.Entry<String,Map<String,TaskConfiguration>>> iterator = runningMapTasks.entrySet().iterator();
          while(iterator.hasNext()){
            Map.Entry<String,Map<String,TaskConfiguration>> entry = iterator.next();
            if(entry.getValue().size() == 0){
              //check if we can/should run the localReduce
              if(jobConf.hasLocalReduce()){
                if(localReduceTasks.get(entry.getKey()).size() > 0){ // pending reduce local tasks
                  result.addAll(invokelocalReduceTasks(entry.getKey()));
                  iterator.remove();
                }
              }
            }else{
              allMapAreFinished = false;
            }
          }
          if(allMapAreFinished){
            if(!jobConf.hasLocalReduce()){
              currentStage = Stage.REDUCE;
              for(Map.Entry<String,Map<String,TaskConfiguration>> entry : federationReduceTasks.entrySet()){
                result.addAll(invokeFederationReduceTasks(entry.getKey()));
              }
              federationReduceTasks.clear();
              runningMapTasks.clear();
            }
          }
        }else{ // all the map tasks are completed.
          if(jobConf.hasLocalReduce()){
            Iterator<Map.Entry<String,Map<String,TaskConfiguration>>> iterator = runningLocalReduceTasks.entrySet().iterator();
            boolean allLocalReduceAreCompleted = true;
            while(iterator.hasNext()){
              Map.Entry<String,Map<String,TaskConfiguration>> entry = iterator.next();
              if(entry.getValue().size() == 0){

              }
              else{
                allLocalReduceAreCompleted = false;
              }
            }
            if (allLocalReduceAreCompleted){
              currentStage = Stage.REDUCE;
              for(Map.Entry<String,Map<String,TaskConfiguration>> entry : federationReduceTasks.entrySet()){
                result.addAll(invokeFederationReduceTasks(entry.getKey()));
              }
              federationReduceTasks.clear();
              runningLocalReduceTasks.clear();
            }
          }else{
            logger.error("Still in Map Stage though no local reduce and all maps are completed");
          }
        }
        break;
      case REDUCE:
        break;
    }
    return result;
  }

  @Override public synchronized Map<String, NoMoreInputMessage> completeTask(String microcloud,String id) {
    Map<String,NoMoreInputMessage> result = new HashedMap();
    if(id.contains("-map-task-")){
      TaskConfiguration configuration = runningMapTasks.get(microcloud).get(id);
      if(configuration == null){
        logger.error("Map task "  + id + " Is not handled by " + jobConf.getJobID() + " in " + nodeManager.getNodeID());
        return result;
      }
      Map<String,TaskConfiguration> completedCloud = completedMapTasks.get(microcloud);
      if(completedCloud == null){
        completedCloud = new HashedMap();
        completedMapTasks.put(microcloud,completedCloud);

      }
      runningMapTasks.get(microcloud).remove(id);
      boolean allMapAreFinished = true;
      Iterator<Map.Entry<String,Map<String,TaskConfiguration>>> iterator = runningMapTasks.entrySet().iterator();
      while(iterator.hasNext()){
        Map.Entry<String,Map<String,TaskConfiguration>> entry = iterator.next();
        if(entry.getValue().size() == 0){
          //check if we can/should run the localReduce
          if(jobConf.hasLocalReduce()){
            if(jobConf.isLocalReducePipeline()){
              result.putAll(getNoMoreInputLocalReduce(microcloud));
            }
          }else{
            if(jobConf.isFederationReducePipeline()) {
              result.putAll(getNoMoreInputFederationReduce(microcloud));
            }
          }
        }
      }

    }else if(id.contains("-local-reduce-task-")){ //LOCAL REDUCE CASE
      TaskConfiguration configuration = runningLocalReduceTasks.get(microcloud).get(id);
      if(configuration == null){
        logger.error("LocalReduce task "  + id + " Is not handled by " + jobConf.getJobID() + " in " + nodeManager.getNodeID());
        return result;
      }
      Map<String,TaskConfiguration> completedCloud = completedLocalReduceTasks.get(microcloud);
      if(completedCloud == null){
        completedCloud = new HashedMap();
        completedLocalReduceTasks.put(microcloud,completedCloud);

      }
      runningLocalReduceTasks.get(microcloud).remove(id);
      if(jobConf.isFederationReducePipeline()){
        if(runningLocalReduceTasks.get(microcloud).size() == 0){
          result.putAll(getNoMoreInputFederationreduce(microcloud));
        }
      }
    }else{
      TaskConfiguration configuration = runningLocalReduceTasks.get(microcloud).get(id);
      if(configuration == null){
        logger.error("FederationReduce task "  + id + " Is not handled by " + jobConf.getJobID() + " in " + nodeManager.getNodeID());
        return result;
      }
      Map<String,TaskConfiguration> completedCloud = completedFederationReduceTasks.get(microcloud);
      if(completedCloud == null){
        completedCloud = new HashedMap();
        completedFederationReduceTasks.put(microcloud,completedCloud);

      }
      runningFederationReduceTasks.get(microcloud).remove(id);
      if(runningFederationReduceTasks.get(microcloud).size() == 0){
        boolean allFederationReducersAreCompleted = true;
        for(Map.Entry<String,Map<String,TaskConfiguration>> entry : runningFederationReduceTasks.entrySet()){
          if(entry.getValue().size() != 0){
            allFederationReducersAreCompleted = false;
            break;
          }
        }
        if(allFederationReducersAreCompleted){
          isCompleted = true;
        }
      }
    }
    return result;
  }

  private Map<? extends String, ? extends NoMoreInputMessage> getNoMoreInputFederationReduce(
      String microcloud) {
    Map<String,NoMoreInputMessage> result = new HashedMap();
    Map<String,TaskConfiguration> tasks = runningFederationReduceTasks.get(microcloud);
    for(Map.Entry<String,TaskConfiguration> entry : tasks.entrySet()){
      result.put(entry.getValue().getNodeID(), new NoMoreInputMessage(entry.getValue().getNodeID(), entry.getValue().getInput(), entry.getValue().getID()));
    }
    return result;
  }

  private Map<? extends String, ? extends NoMoreInputMessage> getNoMoreInputLocalReduce(
      String microcloud) {
    Map<String,NoMoreInputMessage> result = new HashedMap();
    Map<String,TaskConfiguration> tasks = runningLocalReduceTasks.get(microcloud);
    for(Map.Entry<String,TaskConfiguration> entry : tasks.entrySet()){
      result.put(entry.getValue().getNodeID(), new NoMoreInputMessage(entry.getValue().getNodeID(), entry.getValue().getInput(), entry.getValue().getID()));
    }
    return result;
  }

  @Override public boolean isCompleted() {
    return isCompleted;
  }

  @Override public String getID() {
    return jobConf.getJobID();
  }

  @Override public MCConfiguration getConfiguration() {
    return nodeManager.getConfiguration();
  }
}
