package gr.tuc.softnet.core;

import com.google.common.eventbus.EventBus;
import com.sun.org.apache.xalan.internal.lib.NodeInfo;
import gr.tuc.softnet.engine.IDable;
import gr.tuc.softnet.engine.MCNode;
import org.apache.commons.configuration.Configuration;

import java.util.List;
import java.util.SortedMap;

/**
 * Created by vagvaz on 10/02/16.
 */
public interface NodeManager extends IDable, MCNode {
  void updateNodes(Configuration configuration);

  NodeStatus getNodeStatus(String nodeID);

  void killNode(String nodeID);

  void resetNode(String nodeID);

  SortedMap<String, SortedMap<String, NodeStatus>> getMicrocloudInfo();

  SortedMap<String, NodeStatus> getMicrocloudInfo(String siteName);

  SortedMap<String, NodeStatus> getLocalcloudInfo();

  List<NodeStatus> getNodeList();

  List<String> getNodeNames();

  String getLocalcloudName();

  String resolveNode(Object object);

  void initialize(String configurationDirectory);

  void reset();

  List<NodeStatus> getNodeStatus(List<String> microclouds);

  String getNodeID();

  EventBus getEventBus();

}
