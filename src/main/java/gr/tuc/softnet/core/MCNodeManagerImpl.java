package gr.tuc.softnet.core;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import gr.tuc.softnet.engine.JobManager;
import gr.tuc.softnet.engine.MCNode;
import gr.tuc.softnet.engine.TaskManager;
import gr.tuc.softnet.kvs.KVSManager;
import gr.tuc.softnet.netty.MCDataTransport;
import org.apache.commons.configuration.Configuration;

import java.util.List;
import java.util.Map;

/**
 * Created by vagvaz on 11/02/16.
 */
@Singleton
public class MCNodeManagerImpl implements NodeManager, MCNode{
    @Inject
    KVSManager kvsManager;
    @Inject
    MCConfiguration configuration;
    @Inject
    JobManager jobManager;
    @Inject
    TaskManager taskManager;
    @Inject
    NodeManager nodeManager;
    @Inject
    MCDataTransport dataTransport;
    private volatile Object mutex = new Object();



    public void updateNodes(Configuration configuration) {

    }

    public NodeStatus getNodeStatus(String nodeID) {
        return dataTransport.getNodeStatus(nodeID);
    }

    public void killNode(String nodeID) {
        if(nodeID.equals(getID())){
           kill();
        }
        dataTransport.killNode(nodeID);
    }

    public void resetNode(String nodeID) {
        if(nodeID.equals(getID())){
            reset();
        }
        dataTransport.resetNode(nodeID);
    }



    public Map<String, Map<String, NodeStatus>> getMicrocloudInfo() {
        return dataTransport.getMicrocloudInfo();
    }

    public Map<String, NodeStatus> getMicrocloudInfo(String siteName) {
        return dataTransport.getMicrocloudInfo(siteName);
    }

    public Map<String, NodeStatus> getLocalcloudInfo() {
        return getMicrocloudInfo(configuration.getMicroClusterName());
    }

    public String getLocalcloudName() {
        return configuration.getMicroClusterName();
    }

    @Override
    public void waitForExit() {
        synchronized (mutex){
            try {
                mutex.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void kill() {
        System.out.println("Someone decided that I am not needed...Goodbye");
        synchronized (mutex){
            mutex.notify();
        }
    }

    public void initialize(String configurationDirectory) {
        configuration.initialize(configurationDirectory,false);
        dataTransport.initialize();
        jobManager.initialize();
        taskManager.initialize();
        kvsManager.initialize();
    }


    @Override
    public void reset() {
        configuration.initialize(configuration.getBaseDir(),false);
        dataTransport.initialize();
    }

    @Override
    public List<NodeStatus> getNodeStatus(List<String> microclouds) {
        return null;
    }

    public String getID() {
        return configuration.getNodeName();
    }

    public MCConfiguration getConfiguration() {
        return configuration;
    }

}
