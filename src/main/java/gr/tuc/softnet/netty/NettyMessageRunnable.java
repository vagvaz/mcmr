package gr.tuc.softnet.netty;

import gr.tuc.softnet.core.InjectorUtils;
import gr.tuc.softnet.core.NodeManager;
import gr.tuc.softnet.core.PrintUtilities;
import gr.tuc.softnet.engine.JobManager;
import gr.tuc.softnet.engine.TaskManager;
import gr.tuc.softnet.kvs.KVSManager;
import gr.tuc.softnet.netty.messages.MCMessage;
import gr.tuc.softnet.netty.messages.MCMessageWrapper;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.ObjectInputStream;

/**
 * Created by vagvaz on 12/10/15.
 */
public class NettyMessageRunnable implements Runnable {
  Logger log = LoggerFactory.getLogger(NettyMessageRunnable.class);
  MCMessageWrapper nettyMessage;
  ChannelHandlerContext ctx;
  private int replied = 0;
  MCDataTransport transport;
  KVSManager kvsManager;
  NodeManager nodeManager;
  JobManager jobManager;
  TaskManager taskManager;
  public NettyMessageRunnable(ChannelHandlerContext ctx, MCMessageWrapper nettyMessage) {
    this.ctx = ctx;
    this.nettyMessage = nettyMessage;
    transport = InjectorUtils.getInjector().getInstance(MCDataTransport.class);
    kvsManager = InjectorUtils.getInjector().getInstance(KVSManager.class);
    nodeManager = InjectorUtils.getInjector().getInstance(NodeManager.class);
    jobManager = InjectorUtils.getInjector().getInstance(JobManager.class);
    taskManager = InjectorUtils.getInjector().getInstance(TaskManager.class);
  }

//  @Override public void run() {
//    try{
//    String indexName = nettyMessage.getCacheName();
//    //        byte[] bytes = Snappy.uncompress( nettyMessage.getBytes());
//    //        ByteArrayInputStream byteArray = new ByteArrayInputStream(bytes);
//    //        ObjectInputStream ois = new ObjectInputStream(byteArray);
//    //        Object firstObject = ois.readObject();
//    //        if(firstObject instanceof TupleBuffer){
//    try {
//      TupleBuffer buffer = new TupleBuffer(nettyMessage.getBytes());//(TupleBuffer)firstObject;
//      for (Map.Entry<Object, Object> entry : buffer.getBuffer().entrySet()) {
//        IndexManager.addToIndex(indexName, entry.getKey(), entry.getValue());
//      }
//    } catch (Exception e) {
//      PrintUtilities.printAndLog(log,
//          "MessageID " + nettyMessage.getMessageId() + " cache " + nettyMessage.getCacheName() + " bytes lenght" + nettyMessage.getBytes().length);
//      PrintUtilities.printAndLog(log, e.getMessage());
//      PrintUtilities.logStackTrace(log, e.getStackTrace());
//    }
//
//
//    //        } else if(firstObject instanceof String || firstObject instanceof ComplexIntermediateKey){
//    //          Object secondObject = ois.readObject();
//    //          IndexManager.addToIndex(indexName,firstObject,secondObject);
//    //        } else{
//    //          PrintUtilities.printAndLog(log,"Unknown class in NettyMessage " + firstObject.getClass().toString());
//    //        }
//  } catch (Exception e) {
//    e.printStackTrace();
//  }
//  replyForMessage(ctx, nettyMessage);
//  }
@Override public void run() {
  try {
    MCMessageHandler handler = MCMessageHandlerFactory.getHandler(nettyMessage.getType());
    MCMessageWrapper reply = null;
    replyForMessage(ctx, nettyMessage);
    if (handler != null) {
      reply = handler.process(ctx.channel(), nettyMessage);
    } else {
      log.error("Could not find appropriate handler for " + nettyMessage.getType());
    }

    if (reply != null) {
      ctx.writeAndFlush(reply);
    }
  }catch (Exception e){
    e.printStackTrace();
  }
}


  private void replyForMessage(ChannelHandlerContext ctx, MCMessageWrapper nettyMessage) {
    //    ByteBuf buf =  io.netty.buffer.Unpooled.buffer(4);
    //    buf.writeInt(nettyMessage.getMessageId());
    //    ctx.writeAndFlush(buf);
    AcknowledgeMessage acknowledgeMessage = new AcknowledgeMessage(nettyMessage.getRequestId());
    ctx.writeAndFlush(acknowledgeMessage);
    replied++;
  }
}
