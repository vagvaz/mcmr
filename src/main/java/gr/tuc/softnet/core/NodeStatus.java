package gr.tuc.softnet.core;

import com.sun.org.apache.xalan.internal.lib.NodeInfo;
import gr.tuc.softnet.engine.MCNode;
import gr.tuc.softnet.engine.Statusable;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.HierarchicalConfiguration;

/**
 * Created by vagvaz on 10/02/16.
 */
public class NodeStatus implements Statusable {
    String nodeID;
    NodeInfo nodeInfo;
    HierarchicalConfiguration configuration;
    public NodeStatus(String name, Configuration configuration){
        this.nodeID = name;
        this.configuration = new HierarchicalConfiguration();
        this.configuration.append(configuration);
    }
    public NodeStatus(MCNode node){
        nodeID = node.getID();
        configuration = new HierarchicalConfiguration();
        configuration.append(node.getConfiguration());
    }
    public String getID() {
        return nodeID;
    }

    public Configuration getStatus() {
        return configuration;
    }
}
