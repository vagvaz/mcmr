package gr.tuc.softnet.netty;

import com.google.inject.Inject;
import gr.tuc.softnet.core.PrintUtilities;
import gr.tuc.softnet.kvs.KeyValueStore;
import gr.tuc.softnet.netty.messages.MCMessageWrapper;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by vagvaz on 11/25/15.
 */
public class NettyMessageHandler extends ChannelInboundHandlerAdapter {
    private Logger log = LoggerFactory.getLogger(this.getClass());
    @Inject
    MCDataTransport owner;

  ThreadPoolExecutor threadPoolExecutor;
  int received = 0;
  int replied = 0;

  public NettyMessageHandler() {

  }
  public void initialize(){
    threadPoolExecutor = new ThreadPoolExecutor(8,8,1000, TimeUnit.MILLISECONDS,  new LinkedBlockingDeque<Runnable>());
  }

  @Override public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

      if(msg instanceof AcknowledgeMessage){
        AcknowledgeMessage ack  = (AcknowledgeMessage) msg;
          owner.acknowledge(ctx.channel(),ack.getAckMessageId());
      }
      else if(msg instanceof MCMessageWrapper){
        MCMessageWrapper wrapper = (MCMessageWrapper) msg;
        NettyMessageRunnable runnable = new NettyMessageRunnable(ctx,wrapper);
        threadPoolExecutor.submit(runnable);
      }
//      else if(msg instanceof  NettyMessage) {
//          received++;
//
//
//          NettyMessage nettyMessage = (NettyMessage) msg;
//          NettyMessageRunnable runnable = new NettyMessageRunnable(ctx,nettyMessage);
//          threadPoolExecutor.submit(runnable);
//      }
//      else if (msg instanceof KeyRequest){
//        KeyRequest keyRequest = (KeyRequest)msg;
//        if(keyRequest.getValue() == null){
//          KeyValueStore index = owner.getKVS(keyRequest.getCache());
//          if(index != null) {
//            Serializable value = (Serializable) index.get(keyRequest.getKey());
//            keyRequest.setValue(value);
//          } else{
//            keyRequest.setValue("");
//          }
//          ctx.writeAndFlush(keyRequest);
//        }else{
//          System.err.println("key-request-arrived: " + keyRequest.getKey() + "-->" + keyRequest.getValue().toString());
//        }
//      }
    else{
          PrintUtilities.printAndLog(log,"Unknown message Class " + msg.getClass().getCanonicalName().toString());
    }
  }

  @Override public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    ctx.flush();
  }

  @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    super.exceptionCaught(ctx, cause);
  }

  private void replyForMessage(ChannelHandlerContext ctx, NettyMessage nettyMessage) {
//    ByteBuf buf =  io.netty.buffer.Unpooled.buffer(4);
//    buf.writeInt(nettyMessage.getMessageId());
//    ctx.writeAndFlush(buf);
    AcknowledgeMessage acknowledgeMessage = new AcknowledgeMessage(nettyMessage.getMessageId());
    ctx.writeAndFlush(acknowledgeMessage);
    replied++;
  }

}
