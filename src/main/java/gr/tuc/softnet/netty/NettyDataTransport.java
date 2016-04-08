package gr.tuc.softnet.netty;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import gr.tuc.softnet.core.MCConfiguration;
import gr.tuc.softnet.core.NodeStatus;
import gr.tuc.softnet.core.PrintUtilities;
import gr.tuc.softnet.engine.*;
import gr.tuc.softnet.kvs.KVSManager;
import gr.tuc.softnet.kvs.KVSProxy;
import gr.tuc.softnet.kvs.KeyValueStore;
import gr.tuc.softnet.netty.messages.MCMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.configuration.Configuration;
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

/**
 * This class is used to actually transfer/receive data from other nodes.
 * Thus, an array of connections can be used (probably netty has another way of defining sth like that, or we could
 * not use netty if it makes it harder.
 * This is a rough design for this class the API can be changed  however we like it and find it more convenient.
 * Probably a good example for what we will need is this
 * http://netty.io/4.1/xref/io/netty/example/udt/echo/bytes/package-summary.html
 * Created by vagvaz on 10/21/15.
 */
@Singleton
public class NettyDataTransport implements MCDataTransport {
  @Inject 
  MCConfiguration globalConfiguration;
  @Inject
  KVSManager kvsManager;
  @Inject
  JobManager jobManager;
  @Inject
  TaskManager taskManager;
   EventLoopGroup workerGroup;
   EventLoopGroup bossGroup;
   Bootstrap clientBootstrap;
   ServerBootstrap serverBootstrap;
   Set<ChannelFuture> channelFutures;
   Map<String,ChannelFuture> nodes;
   String me;
   Map<Channel,Set<Integer>> pending;
  private  NettyClientChannelInitializer clientChannelInitializer;
  private  NettyServerChannelInitializer serverChannelInitializer;
  private  ChannelFuture serverFuture;
  private  AtomicInteger counter = new AtomicInteger(0);
  private  Map<String,Long> histogram;
  private  Logger log = LoggerFactory.getLogger(NettyDataTransport.class);
  private  boolean nodesInitialized = false;
  private  boolean discard = false;


  @Override
  public  void initialize() {
//    NettyDataTransport.globalConfiguration = globalConfiguration;
    discard =globalConfiguration .conf().getBoolean("transfer.discard",false);
    clientChannelInitializer = new NettyClientChannelInitializer();
    serverChannelInitializer = new NettyServerChannelInitializer();
    pending = new HashMap<>();
    nodes = new TreeMap<>();
    channelFutures = new HashSet<>();
    histogram = new HashMap<>();

    clientBootstrap = new Bootstrap();
    serverBootstrap = new ServerBootstrap();
    workerGroup = new NioEventLoopGroup();
    bossGroup = new NioEventLoopGroup();
    clientBootstrap.group(workerGroup);
//    clientBootstrap.group(workerGroup);
    clientBootstrap.channel(NioSocketChannel.class);
    clientBootstrap.option(ChannelOption.SO_KEEPALIVE,true).handler(clientChannelInitializer);
    serverBootstrap.group(bossGroup,workerGroup).channel(NioServerSocketChannel.class)
        .option(ChannelOption.SO_BACKLOG,128)
        .option(ChannelOption.SO_REUSEADDR,true)
        .childOption(ChannelOption.SO_KEEPALIVE,true)
        //        .childOption(ChannelOption.SO_RCVBUF,2*1024*1024)
        .childHandler(serverChannelInitializer);
    try {
      serverFuture = serverBootstrap.bind(getPort()).sync();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Override
  public synchronized  void initializeNodes(){
    if(nodesInitialized){
      return;
    }
    Configuration componentAddrs = globalConfiguration.conf("componentsAddrs");
    List<String> clouds = new ArrayList(componentAddrs.getList("engine.nodes"));
    Collections.sort(clouds);
    for(String microCloud : clouds ){
      List<String> array = clouds;//TODOcomponentAddrs.getList(microCloud);
      String microCloudIPs = array.get(0);
      String[] URIs = microCloudIPs.split(";");
      for(String URI : URIs){
        String[] parts = URI.split(":");
        String host = parts[0];
        String portString = parts[1];
        boolean ok = false;
        while(!ok){
          try {
            ChannelFuture f = clientBootstrap.connect(host,getPort(portString)).sync();

            ok = true;
            nodes.put(host,f);
            pending.put(f.channel(),new HashSet<Integer>(100));
            channelFutures.add(f);
            histogram.put(host,0L);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    }
    nodesInitialized=true;
  }

  private  int getPort(String portString) {
    Integer result = Integer.parseInt(portString);
    return 10000+(result - 11222);
  }

  private  int getPort() {
    int result = 10000;
//    ClusterInfinispanManager clusterInfinispanManager =
//        (ClusterInfinispanManager) InfinispanClusterSingleton.getInstance().getManager();
//    result += clusterInfinispanManager.getServerPort()-11222;
    result = globalConfiguration.conf().getInt("node.port");
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
  @Override
  public  void send(Channel channel, String indexName, Object key, Object value) {
    send(channel.remoteAddress().toString(),indexName,key,value);
  }

  @Override
  public  void send(String name, String indexName, Object key, Object value) {
    Serializable keySerializable = (Serializable)key;
    Serializable valueSerializable = (Serializable)value;
    try {
      ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(byteArray);
      oos.writeObject(key);
      oos.writeObject(value);
      send(name,indexName,byteArray.toByteArray());
    } catch (IOException e) {
      e.printStackTrace();
    }

  }


  @Override
  public  void send(String target, String cacheName, byte[] bytes) {
    NettyMessage nettyMessage = new NettyMessage(cacheName,bytes,getCounter());
    ChannelFuture f = nodes.get(target);
    if(!discard) {
      pending.get(f.channel()).add(nettyMessage.getMessageId());

      updateHistogram(target, bytes);

      f.channel().write(nettyMessage, f.channel().voidPromise());
    }
  }

  @Override public void send(String target, MCMessage message) {

  }

  private  void updateHistogram(String target, byte[] bytes) {
    Long tmp = histogram.get(target);
    tmp += bytes.length;
    histogram.put(target,tmp);
  }

  private   int getCounter() {
    //    counter = (counter+1) % Integer.MAX_VALUE;
    //    return counter;
    return counter.addAndGet(1);
  }

  /**
   * We might use this method to get all the necessary configuration we might need for initializing NettyKeyValueDataTransfer
   */

  @Override
  public MCConfiguration getGlobalConfig() {
    return globalConfiguration;
  }




  @Override
  public  void spillMetricData() {
    for(Map.Entry<String,Long> entry : histogram.entrySet()){
      PrintUtilities.printAndLog(log,"SPILL: " + entry.getKey() + " " + entry.getValue());
    }
  }

  @Override
  public  void waitEverything() {
    for(Map.Entry<String,ChannelFuture> entry: nodes.entrySet()){
      entry.getValue().channel().flush();
    }
    for(Map.Entry<Channel,Set<Integer>> entry : pending.entrySet()){
      entry.getKey().flush();
      while(entry.getValue().size() > 0 ){
        try {
          PrintUtilities.printAndLog(log,"Waiting " + entry.getKey().remoteAddress() + " " + entry.getValue().size());
          //          PrintUtilities.printList(entry.getValue());
          Thread.sleep(Math.min(Math.max(entry.getValue().size()*100,500),50000));
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  @Override
  public  void acknowledge(Channel owner, int ackMessageId) {
    pending.get(owner).remove(ackMessageId);
  }

  @Override
  public  Map<String, ChannelFuture> getNodes() {
    return nodes;
  }

  @Override
  public <K, V> V remoteGet(String nodeName, K key) {
    return null;
  }

  @Override
  public int remoteSize(String node) {
    return 0;
  }

  @Override
  public <K> boolean remoteContains(String node, K key) {
    return false;
  }

  @Override
  public void cancelTask(String id, String taskID) {

  }



  @Override
  public boolean startJob(List<NodeStatus> nodes, JobConfiguration jobConfiguration) {
    return false;
  }

  @Override
  public boolean startJob(JobConfiguration jobConfiguration) {
    return false;
  }

  @Override
  public boolean cancelJob(List<NodeStatus> nodes, String jobID) {
    return false;
  }

  @Override
  public boolean cancelJob(String jobID) {
    return false;
  }

  @Override
  public void completedJob(String jobID) {

  }

  @Override
  public JobStatus getJobStatus(String jobID) {
    return null;
  }

  @Override
  public void waitForCompletion(String jobID) {

  }

  @Override
  public void taskCompleted(String jobID, String id) {

  }

  @Override
  public <K extends WritableComparable, V extends Writable> KeyValueStore<K, V> createKVS(String name, Configuration configuration) {
    return null;
  }

  @Override
  public boolean destroyKVS(String name) {
    return false;
  }

  @Override
  public <K extends WritableComparable, V extends Writable> KeyValueStore<K, V> getKVS(String name) {
    return null;
  }

  @Override
  public <K extends WritableComparable, V extends Writable> KVSProxy<K, V> getKVSProxy(String name) {
    return null;
  }

  @Override
  public void updateNodes(Configuration configuration) {

  }

  @Override
  public NodeStatus getNodeStatus(String nodeID) {
    return null;
  }

  @Override
  public void killNode(String nodeID) {

  }

  @Override
  public void resetNode(String nodeID) {

  }

  @Override
  public Map<String, Map<String, NodeStatus>> getMicrocloudInfo() {
    return null;
  }

  @Override
  public Map<String, NodeStatus> getMicrocloudInfo(String siteName) {
    return null;
  }

  @Override
  public Map<String, NodeStatus> getLocalcloudInfo() {
    return null;
  }

  @Override
  public String getLocalcloudName() {
    return null;
  }

  @Override
  public void initialize(String configurationDirectory) {

  }

  @Override
  public void reset() {

  }

  @Override
  public List<NodeStatus> getNodeStatus(List<String> microclouds) {
    return null;
  }

//  @Override
//  public boolean startTask(Configuration taskConfiguration) {
//    return false;
//  }

  @Override
  public boolean startTask(TaskConfiguration taskConfiguration) {
    return false;
  }

  @Override
  public TaskStatus getTaskStatus(String taskID) {
    return null;
  }

  @Override
  public boolean cancelTask(String taskID) {
    return false;
  }

  @Override
  public void waitForTaskCompletion(String taskID) {

  }

  @Override
  public String getID() {
    return null;
  }

  @Override
  public MCConfiguration getConfiguration() {
    return null;
  }

  @Override
  public void waitForExit() {

  }

  @Override
  public void kill() {

  }
}
