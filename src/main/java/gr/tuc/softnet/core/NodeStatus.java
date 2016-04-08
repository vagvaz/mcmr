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
    HierarchicalConfiguration configuration;
    public NodeStatus(String name, Configuration configuration){
        this.nodeID = name;
        this.configuration = new HierarchicalConfiguration();
        this.configuration.append(configuration);
    }
    public NodeStatus(MCNode node){
        nodeID = node.getID();
        configuration = new HierarchicalConfiguration();
        configuration.append(node.getConfiguration().conf());
    }
    public String getID() {
        return nodeID;
    }

    public Configuration getStatus() {
        return configuration;
    }

    public String getMicrocloud(){
        return configuration.getString(ConfStringConstants.MICRO_CLOUD);
    }

    public void setMicrocloud(String microcloud){
        configuration.setProperty(ConfStringConstants.MICRO_CLOUD,microcloud);
    }
}
