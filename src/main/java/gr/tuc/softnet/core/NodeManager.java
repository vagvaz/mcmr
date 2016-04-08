package gr.tuc.softnet.core;

import com.sun.org.apache.xalan.internal.lib.NodeInfo;
import gr.tuc.softnet.engine.IDable;
import gr.tuc.softnet.engine.MCNode;
import org.apache.commons.configuration.Configuration;

import java.util.List;
import java.util.Map;

/**
 * Created by vagvaz on 10/02/16.
 */
public interface NodeManager extends IDable,MCNode   {
    void updateNodes(Configuration configuration);
    NodeStatus getNodeStatus(String nodeID);
    void killNode(String nodeID);
    void resetNode(String nodeID);
    Map<String, Map<String,NodeStatus>> getMicrocloudInfo();
    Map<String,NodeStatus> getMicrocloudInfo(String siteName);
    Map<String,NodeStatus> getLocalcloudInfo();
    String getLocalcloudName();
    void initialize(String configurationDirectory);
    void reset();

    List<NodeStatus> getNodeStatus(List<String> microclouds);

    String getNodeID();
}
