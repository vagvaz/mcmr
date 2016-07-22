package gr.tuc.softnet.core;

import com.google.common.eventbus.EventBus;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import gr.tuc.softnet.engine.JobManager;
import gr.tuc.softnet.engine.MCNode;
import gr.tuc.softnet.engine.TaskManager;
import gr.tuc.softnet.kvs.KVSManager;
import gr.tuc.softnet.netty.MCDataTransport;
import org.apache.commons.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

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
    MCDataTransport dataTransport;
    private volatile Object mutex = new Object();
    List<String> nodeNames;

    public void initialize(String configurationDirectory) {

        configuration.initialize(configurationDirectory,false);

        dataTransport.initialize();
        nodeNames = getNodeNames();
        jobManager.initialize();
        taskManager.initialize();
        kvsManager.initialize();
    }


    @Override
    public void reset() {
        configuration.initialize(configuration.getBaseDir(),false);
        dataTransport.initialize();
    }

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


    @Override
    public SortedMap<String, SortedMap<String, NodeStatus>> getMicrocloudInfo() {
        return dataTransport.getMicrocloudInfo();
    }

    @Override
    public SortedMap<String, NodeStatus> getMicrocloudInfo(String siteName) {
        return dataTransport.getMicrocloudInfo(siteName);
    }

    @Override
    public SortedMap<String, NodeStatus> getLocalcloudInfo() {
        return getMicrocloudInfo(configuration.getMicroClusterName());
    }

    @Override public List<NodeStatus> getNodeList() {
        List<NodeStatus> result = new ArrayList<>();
        for(Map.Entry<String,SortedMap<String,NodeStatus>> cloudEntry : dataTransport.getMicrocloudInfo().entrySet()){
            for(Map.Entry<String,NodeStatus> nodeEntry : cloudEntry.getValue().entrySet()){
                result.add(nodeEntry.getValue());
            }
        }
        return result;
    }

    @Override public List<String> getNodeNames() {
        List<String> result = new ArrayList<>();
        for(Map.Entry<String,SortedMap<String,NodeStatus>> cloudEntry : dataTransport.getMicrocloudInfo().entrySet()){
            for(Map.Entry<String,NodeStatus> nodeEntry : cloudEntry.getValue().entrySet()){
                result.add(nodeEntry.getKey());
            }
        }
        return result;
    }

    public String getLocalcloudName() {
        return configuration.getMicroClusterName();
    }

    @Override public String resolveNode(Object object) {
        if(nodeNames != null && nodeNames.size() > 0){
            int index = Math.abs(object.hashCode());
            return nodeNames.get(index % nodeNames.size());
        }
        return null;
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


    @Override
    public List<NodeStatus> getNodeStatus(List<String> microclouds) {
        return null;
    }

    @Override public String getNodeID() {
        return getID();
    }

    @Override public EventBus getEventBus() {
        return dataTransport.getEventBus();
    }

    public String getID() {
        return configuration.getNodeName();
    }

    public MCConfiguration getConfiguration() {
        return configuration;
    }

}
