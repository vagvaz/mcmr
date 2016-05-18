package gr.tuc.softnet.core;

import com.sun.org.apache.xalan.internal.lib.NodeInfo;
import com.sun.org.apache.xalan.internal.xsltc.util.IntegerArray;
import gr.tuc.softnet.engine.MCNode;
import gr.tuc.softnet.engine.Statusable;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.HierarchicalConfiguration;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Iterator;

/**
 * Created by vagvaz on 10/02/16.
 */
public class NodeStatus implements Statusable, Serializable {
    HierarchicalConfiguration configuration;

    public NodeStatus(String nodeID, String cloud) {
        configuration = new HierarchicalConfiguration();
        String ip_port = nodeID;
        String[] splits = ip_port.split(":");
        String ip = splits[0];
        String port = splits[1];
        setIP(ip);
        setPort(port);
        nodeID = nodeID;
        setNodeID(nodeID);
    }

    public String getID() {
        return getNodeID();
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
    public String getIP(){
        return (String) configuration.getProperty(ConfStringConstants.IP);
    }

    public int getPort(){
        return (int) configuration.getProperty(ConfStringConstants.PORT);
    }
    public void setIP(String ip){
        configuration.setProperty(ConfStringConstants.IP,ip);
    }

    public void setPort(String port){
        configuration.setProperty(ConfStringConstants.PORT, Integer.parseInt(port));
    }

    public void setPort(int port){
        configuration.setProperty(ConfStringConstants.PORT,port);
    }

    public void setNodeID(String nodeID) {
        configuration.setProperty(ConfStringConstants.NODE_ID,nodeID);
    }

    public String getNodeID() {
        return (String) configuration.getProperty(ConfStringConstants.NODE_ID);
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        Iterator<String> iterator =  configuration.getKeys();
        int counter = configuration.getRootNode().getChildren().size();
        out.writeInt(counter);
        while(iterator.hasNext()){
            if(counter-- < 0)
            {
                try {
                    throw new Exception("counter neg");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            String key = iterator.next();
            out.writeObject(key);
            out.writeObject(configuration.getProperty(key));
        }

    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        int sz = in.readInt();
        while(sz > 0){
            sz--;
            String key = (String) in.readObject();
            Object value = in.readObject();
            configuration.setProperty(key,value);
        }
    }

    private void readObjectNoData() throws ObjectStreamException {

    }
}
