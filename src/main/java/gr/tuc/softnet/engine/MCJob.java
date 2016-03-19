package gr.tuc.softnet.engine;

import gr.tuc.softnet.core.NodeStatus;

import java.util.List;
import java.util.Map;

/**
 * Created by vagvaz on 10/02/16.
 */
public interface MCJob extends IDable {
    void setNodes(List<NodeStatus> nodes);

    List<TaskConfiguration> getTaskConfigurations();

    List<? extends NodeStatus> getNodes();

    void addTask(NodeStatus node, TaskConfiguration taskConfiguration);

    Iterable<Map.Entry<NodeStatus, TaskConfiguration>> getTasks();

    JobStatus getStatus();

    void waitForCompletion();
}
