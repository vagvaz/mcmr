package gr.tuc.softnet.netty;

import gr.tuc.softnet.core.MCConfiguration;
import gr.tuc.softnet.core.NodeManager;
import gr.tuc.softnet.engine.JobManager;
import gr.tuc.softnet.engine.TaskManager;
import gr.tuc.softnet.kvs.KVSManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.util.Map;

/**
 * Created by vagvaz on 10/02/16.
 */
public interface MCDataTransport extends KVSManager, TaskManager, NodeManager, JobManager {
    void initialize();

    void initializeNodes();

    void send(Channel channel, String indexName, Object key, Object value);

    void send(String name, String indexName, Object key, Object value);

    void send(String target, String cacheName, byte[] bytes);

    MCConfiguration getGlobalConfig();

    void spillMetricData();

    void waitEverything();

    void acknowledge(Channel owner, int ackMessageId);

    Map<String, ChannelFuture> getNodes();

    <K,V> V remoteGet(String nodeName, K key);

    int remoteSize(String node);

    <K> boolean remoteContains(String node, K key);

    void cancelTask(String id, String taskID);
}
