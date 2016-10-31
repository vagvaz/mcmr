package gr.tuc.softnet.netty;

import com.google.common.eventbus.EventBus;
import gr.tuc.softnet.core.MCConfiguration;
import gr.tuc.softnet.core.NodeStatus;
import gr.tuc.softnet.engine.JobConfiguration;
import gr.tuc.softnet.engine.JobStatus;
import gr.tuc.softnet.engine.MCJobProxy;
import gr.tuc.softnet.engine.TaskConfiguration;
import gr.tuc.softnet.kvs.KVSConfiguration;
import gr.tuc.softnet.kvs.KeyValueStore;
import gr.tuc.softnet.netty.messages.KVSDescriptionRequest;
import gr.tuc.softnet.netty.messages.KVSDescriptionResponse;
import gr.tuc.softnet.netty.messages.MCMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 * Created by vagvaz on 10/02/16.
 */
public interface MCDataTransport  {
    void initialize();

    void initializeNodes();

    void send(Channel channel, String indexName, Object key, Object value);

    void send(String name, String indexName, Object key, Object value);

    void flush();

    void send(String target, String cacheName, byte[] bytes);

    void send(String target, MCMessage message);
    void sendAndFlush(String target, MCMessage message);

    long sendRequest(String target, MCMessage message);

    void sendRequestResponse(String target, MCMessage message, long requestID);

    MCConfiguration getGlobalConfig();

    void spillMetricData();

    void waitEverything();

    void acknowledge(Channel owner, long ackMessageId);

    Map<String, ChannelFuture> getNodes();

    <K extends WritableComparable,V extends Writable> V remoteGet(String kvsName,String nodeName, K key);

    int remoteSize(String name, String node);

    <K extends WritableComparable> boolean remoteContains(String kvsName,String node, K key);

    void cancelTask(String id, String taskID);

    MCConfiguration getConfiguration();

    KVSConfiguration createKVS(String name, KVSConfiguration kvsConfiguration);

    void startTask(TaskConfiguration task);

    boolean cancelJob(String node, String jobID, List<String> nodes);

    JobStatus getJobStatus(String id, String jobID);

    void taskCompleted(TaskConfiguration task);

    KeyValueStore getKVS(String cache);

    NodeStatus getNodeStatus(String nodeID);

    void killNode(String nodeID);

    void resetNode(String nodeID);

    SortedMap<String, SortedMap<String,NodeStatus>> getMicrocloudInfo();

    SortedMap<String,NodeStatus> getMicrocloudInfo(String siteName);

    void batchSend(String nodeName, String kvsName, byte[] bytes,
      Class<? extends WritableComparable> keyClass, Class<? extends Writable> valueClass, boolean b);

    MCJobProxy submitJob(JobConfiguration configuration, String microcloud);

    MCJobProxy submitJob(JobConfiguration configuration);

    void waitForJobCompletion(String id, String jobID);

    KVSDescriptionResponse getKVSInfo(String kvsStoreMaster, KVSDescriptionRequest request);

    EventBus getEventBus();

    String getNodeName(Channel channel);

    public void addClient(Channel channel, String nodeName);

    void jobCompleted(JobConfiguration configuration);

  KVSConfiguration remoteCreateKVS(String node, KVSConfiguration kvsConfiguration);
}
