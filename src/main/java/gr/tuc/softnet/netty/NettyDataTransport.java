package gr.tuc.softnet.netty;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import gr.tuc.softnet.core.MCConfiguration;
import gr.tuc.softnet.core.NodeStatus;
import gr.tuc.softnet.core.PrintUtilities;
import gr.tuc.softnet.core.StringConstants;
import gr.tuc.softnet.engine.*;
import gr.tuc.softnet.kvs.KVSConfiguration;
import gr.tuc.softnet.kvs.KVSManager;
import gr.tuc.softnet.kvs.KeyValueStore;
import gr.tuc.softnet.netty.messages.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is used to actually transfer/receive data from other nodes.
 * Thus, an array of connections can be used (probably netty has another way of defining sth like that, or we could
 * not use netty if it makes it harder.
 * This is a rough design for this class the API can be changed  however we like it and find it more convenient.
 * Probably a good example for what we will need is this
 * http://netty.io/4.1/xref/io/netty/example/udt/echo/bytes/package-summary.html
 * Created by vagvaz on 10/21/15.
 */
@Singleton public class NettyDataTransport implements MCDataTransport {
  @Inject MCConfiguration globalConfiguration;
  @Inject KVSManager kvsManager;
  @Inject JobManager jobManager;
  @Inject TaskManager taskManager;
  EventLoopGroup workerGroup;
  EventLoopGroup bossGroup;
  Bootstrap clientBootstrap;
  ServerBootstrap serverBootstrap;
  Set<ChannelFuture> channelFutures;
  Map<String, ChannelFuture> nodes;
  String me;
  Map<Channel, Set<Long>> pending;
  private NettyClientChannelInitializer clientChannelInitializer;
  private NettyServerChannelInitializer serverChannelInitializer;
  private ChannelFuture serverFuture;
  private AtomicInteger counter = new AtomicInteger(0);
  private Map<String, Long> histogram;
  private Logger log = LoggerFactory.getLogger(NettyDataTransport.class);
  private boolean nodesInitialized = false;
  private boolean discard = false;
  private AtomicLong requestID = new AtomicLong(0);
  private Map<Long, MCMessage> requests;
  private volatile Map<Long, Object> mutexes;
  private SortedMap<String, SortedMap<String, NodeStatus>> cloudInfo;
  private Map<Channel, String> clientMap;
  private EventBus eventBus;
  private NodeStatus mineNodeStatus;
  String baseDir;

  @Override public void initialize() {
    //    NettyDataTransport.globalConfiguration = globalConfiguration;
    clientMap = new HashedMap();
    eventBus = new EventBus(getConfiguration().getNodeName());
    eventBus.register(this);
    discard = globalConfiguration.conf().getBoolean("transfer.discard", false);
    clientChannelInitializer = new NettyClientChannelInitializer();
    serverChannelInitializer = new NettyServerChannelInitializer();
    pending = new HashMap<>();
    nodes = new TreeMap<>();
    channelFutures = new HashSet<>();
    histogram = new HashMap<>();
    mutexes = new HashedMap();
    requests = new HashedMap();

    clientBootstrap = new Bootstrap();
    serverBootstrap = new ServerBootstrap();
    workerGroup = new NioEventLoopGroup();
    bossGroup = new NioEventLoopGroup();
    clientBootstrap.group(workerGroup);
    //    clientBootstrap.group(workerGroup);
    clientBootstrap.channel(NioSocketChannel.class);
    clientBootstrap.option(ChannelOption.SO_KEEPALIVE, true).handler(clientChannelInitializer);
    serverBootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
      .option(ChannelOption.SO_BACKLOG, 128).option(ChannelOption.SO_REUSEADDR, true)
      .childOption(ChannelOption.SO_KEEPALIVE, true)
      //        .childOption(ChannelOption.SO_RCVBUF,2*1024*1024)
      .childHandler(serverChannelInitializer);
    cloudInfo = new TreeMap<>();
    HierarchicalConfiguration conf =
      (HierarchicalConfiguration) globalConfiguration.conf().getConfiguration(0);
    List<HierarchicalConfiguration> mcs = conf.configurationsAt("network.mc");
    for (HierarchicalConfiguration c : mcs) {
      SortedMap<String, NodeStatus> newCloud = new TreeMap<>();
      String cloud = c.getString("name");
      cloudInfo.put(cloud, newCloud);
      List<Object> nodes = c.getList("node");
      for (Object node : nodes) {
        NodeStatus status = new NodeStatus((String) node, cloud);
        newCloud.put(status.getID(), status);
        this.nodes.put(status.getNodeID(), null);
      }
    }

    try {
      String ip = getIP();
      int port = getPort();
      if (!ip.equals("")) {
        serverFuture = serverBootstrap.bind(ip, port).sync();
        mineNodeStatus = new NodeStatus(ip + ":" + port, getConfiguration().getMicroClusterName());
        me = mineNodeStatus.getNodeID();
      } else {
        me = "";
        mineNodeStatus = null;
      }
      initializeNodes();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

    @Override public synchronized void initializeNodes () {
      if (nodesInitialized) {
        return;
      }
      for (String cloud : cloudInfo.keySet()) {
        for (NodeStatus status : cloudInfo.get(cloud).values()) {


          boolean ok = false;
          while (!ok) {
            try {
              ChannelFuture f = clientBootstrap.connect(status.getIP(), status.getPort()).sync();

              ok = true;
              this.nodes.put(status.getNodeID(), f);

              pending.put(f.channel(), new HashSet<Long>(100));
              channelFutures.add(f);
              histogram.put(status.getNodeID(), 0L);
              if (nodes.containsKey(me)) {
                sendAnnouncement(status.getNodeID());
              }
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      }

      nodesInitialized = true;
    }

  private void sendAnnouncement(String nodeID) {
    NodeAnnouncementMessage message = new NodeAnnouncementMessage(me);
    sendAndFlush(nodeID, message);
  }

  private int getPort(String portString) {
    Integer result = Integer.parseInt(portString);
    return 10000 + (result - 11222);
  }

  private int getPort() {
    Integer result = 10000;
    //    ClusterInfinispanManager clusterInfinispanManager =
    //        (ClusterInfinispanManager) InfinispanClusterSingleton.getInstance().getManager();
    //    result += clusterInfinispanManager.getServerPort()-11222;
    result = globalConfiguration.conf().getInt("node.port");
    if (result == null) {
      result = -1;
    }
    return result;
  }

  private String getIP() {
    String result = globalConfiguration.conf().getString("node.public_ip");
    if (result == null) {
      result = "";
    }
    return result;
  }

  /**
   * The method is called to send data to another node of course if the put is local then the data are directly put into
   * The queue for the respective index.
   * functionality pseudoCode
   * if(channel.equals( me))
   * IndexManager.put(indexName,key,value);
   * else
   * Message msg = new NettyMessage (indexName,key,value) this class is Serializable)
   * channel.write(msg)
   * The actual implementation should be a bit more comples as ( if netty does not already does that each connection should
   * have its own thread. As a by product of this thre might be a need for an individual queue.
   *
   * @param channel
   * @param indexName
   * @param key
   * @param value
   */
  @Override public void send(Channel channel, String indexName, Object key, Object value) {
    //DEPRECATED
    send(channel.remoteAddress().toString(), indexName, key, value);
  }

  @Override public void send(String name, String indexName, Object key, Object value) {
    Serializable keySerializable = (Serializable) key;
    Serializable valueSerializable = (Serializable) value;
    try {
      ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(byteArray);
      oos.writeObject(key);
      oos.writeObject(value);
      send(name, indexName, byteArray.toByteArray());
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  @Override public void flush() {
    for (ChannelFuture future : nodes.values()) {
      future.channel().flush();
    }
  }

  @Override public void send(String target, String cacheName, byte[] bytes) {
    //    NettyMessage nettyMessage = new NettyMessage(cacheName,bytes,getCounter());
    //    ChannelFuture f = nodes.get(target);
    //    if(!discard) {
    //      pending.get(f.channel()).add(nettyMessage.getMessageId());
    //
    //      updateHistogram(target, bytes);
    //
    //      f.channel().write(nettyMessage, f.channel().voidPromise());
    //    }
  }

  @Override public void send(String target, MCMessage message) {
    MCMessageWrapper wrapper = new MCMessageWrapper(message, getRequestID());
    ChannelFuture f = nodes.get(target);
    {
      pending.get(f.channel()).add(wrapper.getRequestId());
      updateHistogram(target, wrapper.bytesSize());
      f.channel().write(wrapper, f.channel().voidPromise());
    }
  }

  public void sendAndFlush(String target, MCMessage message) {
    MCMessageWrapper wrapper = new MCMessageWrapper(message, getRequestID());
    ChannelFuture f = nodes.get(target);
    {
      pending.get(f.channel()).add(wrapper.getRequestId());
      updateHistogram(target, wrapper.bytesSize());
      f.channel().write(wrapper, f.channel().voidPromise());
      f.channel().flush();
    }
  }

  @Subscribe
  @AllowConcurrentEvents
  public void setRequestResponse(RequestResponseEvent event) {
    try {
      long requestID = event.getRequestID();
      MCMessage message = event.getMessage();
      String node = event.getNode();
      Object mutex = mutexes.containsKey(requestID);
      synchronized (mutex) {
        if (mutex != null) {
          requests.put(requestID, message);
          mutex.notify();
        } else {
          log.error("Received redudant request response " + requestID + " from " + node);
        }
      }
    }
    catch(Exception e){
      e.printStackTrace();
    }
  }

  @Override public long sendRequest(String target, MCMessage message) {
    MCMessageWrapper wrapper = new MCMessageWrapper(message, getRequestID());

    ChannelFuture future = nodes.get(target);
    pending.get(future.channel()).add(wrapper.getRequestId());
    mutexes.put(wrapper.getRequestId(), new Object());
    future.channel().write(wrapper);
    future.channel().flush();
    return wrapper.getRequestId();
  }

  @Override public void sendRequestResponse(String target, MCMessage message, long requestID) {
    MCMessageWrapper wrapper = new MCMessageWrapper(message, -requestID);

    ChannelFuture future = nodes.get(target);
    pending.get(future.channel()).add(wrapper.getRequestId());
    future.channel().write(wrapper);
    future.channel().flush();
  }

  private void updateHistogram(String target, byte[] bytes) {
    Long tmp = histogram.get(target);
    tmp += bytes.length;
    histogram.put(target, tmp);
  }

  private int getCounter() {
    //    counter = (counter+1) % Integer.MAX_VALUE;
    //    return counter;
    return counter.addAndGet(1);
  }

  /**
   * We might use this method to get all the necessary configuration we might need for initializing NettyKeyValueDataTransfer
   */

  @Override public MCConfiguration getGlobalConfig() {
    return globalConfiguration;
  }



  @Override public void spillMetricData() {
    for (Map.Entry<String, Long> entry : histogram.entrySet()) {
      PrintUtilities.printAndLog(log, "SPILL: " + entry.getKey() + " " + entry.getValue());
    }
  }

  @Override public void waitEverything() {
    for (Map.Entry<String, ChannelFuture> entry : nodes.entrySet()) {
      entry.getValue().channel().flush();
    }
    for (Map.Entry<Channel, Set<Long>> entry : pending.entrySet()) {
      entry.getKey().flush();
      while (entry.getValue().size() > 0) {
        try {
          PrintUtilities.printAndLog(log,
            "Waiting " + entry.getKey().remoteAddress() + " " + entry.getValue().size());
          //          PrintUtilities.printList(entry.getValue());
          Thread.sleep(Math.min(Math.max(entry.getValue().size() * 100, 500), 50000));
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  @Override public void acknowledge(Channel owner, long ackMessageId) {
    pending.get(owner).remove(ackMessageId);
  }

  @Override public Map<String, ChannelFuture> getNodes() {
    return nodes;
  }

  @Override public <K extends WritableComparable, V extends Writable> V remoteGet(String kvsName,
    String nodeName, K key) {
    KVSGet<K> message = new KVSGet<>(kvsName, key, (Class<K>) key.getClass());
    long requestID = sendRequest(nodeName, message);
    MCMessage response = getRequestResult(requestID);
    KVSGetResponse getResponse = (KVSGetResponse) response;
    return (V) getResponse.getValue();
  }

  private MCMessage getRequestResult(long requestID) {
    MCMessage response = requests.get(requestID);
    if (response == null) {
      waitForResult(requestID);
    }
    return requests.get(requestID);
  }

  private void waitForResult(long requestID) {
    Object mutex = mutexes.get(requestID);
    synchronized (mutex) {
      while (!requests.containsKey(requestID)) {
        try {
          mutex.wait(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }


  @Override public int remoteSize(String name, String node) {
    return 0;
  }

  @Override
  public <K extends WritableComparable> boolean remoteContains(String node, String kvsName, K key) {
    KVSContains message = new KVSContains(kvsName, key, key.getClass());
    long responseID = sendRequest(node, message);
    waitForResult(responseID);
    return false;
  }

  @Override public void cancelTask(String id, String taskID) {

  }

  @Override public MCConfiguration getConfiguration() {
    return this.globalConfiguration;
  }

  @Override
  public KVSConfiguration remoteCreateKVS(String node, KVSConfiguration kvsConfiguration) {
    RemoteKVSCreate request = new RemoteKVSCreate(kvsConfiguration.getName(),kvsConfiguration);
    long requestId = sendRequest(node,request);
    KVSDescriptionResponse response = (KVSDescriptionResponse) getRequestResult(requestId);
    return response.getConfiguration();
  }

  @Override public KVSConfiguration createKVS(String cacheName, KVSConfiguration kvsConfiguration) {
    List<String> clouds = kvsConfiguration.getClouds();
    List<Long> createRequests = new ArrayList<>();
    KVSConfiguration result = null;
    if (clouds.size() == 0) {
      clouds.addAll(this.cloudInfo.keySet());
    }
    for (String cloud : clouds) {
      Map<String, NodeStatus> cloudNodes = getMicrocloudInfo(cloud);
      if (cloudNodes == null || cloudNodes.size() == 0) {
        continue;
      }

      for (String node : cloudNodes.keySet()) {
        if (!node.equals(me)) {
          KVSCreate message = new KVSCreate(cacheName, kvsConfiguration);
          createRequests.add(sendRequest(node, message));
        } else {
          KVSConfiguration conf = kvsManager.bootstrapKVS(kvsConfiguration);
          if (node.equals(me)) {
            result = conf;
          }
        }
      }
    }
    for (Long request : createRequests) {
      KVSDescriptionResponse response = (KVSDescriptionResponse) getRequestResult(request);
      result = response.getConfiguration();
    }
    return result;
  }

  @Override public void startTask(TaskConfiguration task) {
    StartTask taskMessage = new StartTask(task);
    this.sendAndFlush(task.getNodeTargetNode(),taskMessage);
//    sendRequest(task.getNodeTargetNode(), taskMessage);
  }

  @Override public boolean cancelJob(String node, String jobID, List<String> nodes) {
    return false;
  }


  @Override public JobStatus getJobStatus(String id, String jobID) {
    return null;
  }

  @Override public void taskCompleted(TaskConfiguration task) {
    TaskCompleted completedTask = new TaskCompleted(task);
    this.sendAndFlush(task.getCoordinator(), completedTask);
  }

  @Override public KeyValueStore getKVS(String cache) {
    return null;
  }

  @Override public NodeStatus getNodeStatus(String nodeID) {
    return null;
  }

  @Override public void killNode(String nodeID) {

  }

  @Override public void resetNode(String nodeID) {

  }

  @Override public SortedMap<String, SortedMap<String, NodeStatus>> getMicrocloudInfo() {
    return this.cloudInfo;
  }

  @Override public SortedMap<String, NodeStatus> getMicrocloudInfo(String siteName) {
    return this.cloudInfo.get(siteName);
  }

  @Override public void batchSend(String nodeName, String kvsName, byte[] bytes,
    Class<? extends WritableComparable> keyClass, Class<? extends Writable> valueClass, boolean flush) {

    ChannelFuture channel = nodes.get(nodeName);
    KVSBatchPut message = new KVSBatchPut(keyClass, valueClass, bytes, kvsName);
    long requestId = sendRequest(nodeName, message);
    EmptyKVSResponse response = (EmptyKVSResponse) getRequestResult(requestId);
//    MCMessageWrapper wrapper = new MCMessageWrapper(message, getRequestID());
//    channel.channel().write(wrapper, channel.channel().voidPromise());
//    if(flush){
//      channel.channel().flush();
//    }
  }

  @Override public MCJobProxy submitJob(JobConfiguration configuration, String microcloud) {
    NodeStatus node = cloudInfo.get(microcloud).values().iterator().next();
    MCJobProxy jobProxy = new MCJobProxy(node, configuration, jobManager);
    SubmitJob newJob = new SubmitJob(me, configuration);
    this.sendAndFlush(node.getID(), newJob);
    return jobProxy;
  }

  @Override public MCJobProxy submitJob(JobConfiguration configuration) {
    String microcloud = null;
    if (cloudInfo.size() > 0) {
      microcloud = cloudInfo.keySet().iterator().next();
    } else {
      return null;
    }
    return submitJob(configuration, microcloud);
  }

  @Override public void waitForJobCompletion(String id, String jobID) {

  }

  @Override
  public KVSDescriptionResponse getKVSInfo(String kvsStoreMaster, KVSDescriptionRequest request) {
    if (nodes.containsKey(kvsStoreMaster)) {
      long requestID = sendRequest(kvsStoreMaster, request);
      KVSDescriptionResponse response = (KVSDescriptionResponse) getRequestResult(requestID);
      return response;
    }
    KVSDescriptionResponse result = new KVSDescriptionResponse(new KVSConfiguration());
    return result;
  }

  @Override public EventBus getEventBus() {
    return eventBus;
  }

  @Override public String getNodeName(Channel channel) {
    String result = clientMap.get(channel);
    if(result == null) {
      result = "";
    }
    return result;
  }

  @Override public void addClient(Channel channel, String nodeName) {
    clientMap.put(channel, nodeName);
  }

  @Override public void jobCompleted(JobConfiguration completedJob) {
    JobCompleted completed = new JobCompleted(completedJob);
//    this.sendRequestResponse(completedJob.getClient(),completed, completedJob.getLong(StringConstants.REQUEST_NUMBER));
    this.sendAndFlush(completedJob.getClient(),completed);
  }



  public long getRequestID() {
    long result =  requestID.incrementAndGet();
    return result;
  }
}
