package gr.tuc.softnet.engine;

import gr.tuc.softnet.core.NodeStatus;
import gr.tuc.softnet.kvs.KVSConfiguration;
import gr.tuc.softnet.netty.messages.NoMoreInputMessage;
import org.apache.hadoop.mapred.JobConf;

import java.util.List;
import java.util.Map;

/**
 * Created by vagvaz on 10/02/16.
 */
public interface MCJob extends IDable {
    void setNodes(List<NodeStatus> nodes);
    void initialize(JobConfiguration jobConf, Map<String,NodeStatus> clouds);
//    List<TaskConfiguration> getTaskConfigurations();

    List<? extends NodeStatus> getNodes();

    void addTask(NodeStatus node, TaskConfiguration taskConfiguration);

    Iterable<Map.Entry<NodeStatus, TaskConfiguration>> getTasks();

    JobStatus getStatus();

    void waitForCompletion();

    KVSConfiguration getMapOuputConfiguration();

    JobConfiguration getJobConfiguration();

    KVSConfiguration getLocalReduceOutputConfiguration();

    KVSConfiguration getFederationReduceOutput();

    List<TaskConfiguration> getReadyToRunTaskConfigurations();

    Map<String,NoMoreInputMessage> completeTask(String microcloud, String id);

    boolean isCompleted();
}
