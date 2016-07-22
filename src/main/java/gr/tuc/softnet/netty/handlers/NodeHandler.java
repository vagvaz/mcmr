package gr.tuc.softnet.netty.handlers;

import gr.tuc.softnet.netty.MCMessageHandler;
import gr.tuc.softnet.netty.messages.MCMessageWrapper;
import io.netty.channel.Channel;

/**
 * Created by vagvaz on 18/05/16.
 */
public class NodeHandler extends MCMessageHandler {
  public NodeHandler(){
    super();
  }

  @Override public MCMessageWrapper process(Channel node, MCMessageWrapper wrapper) {
    return null;
  }

}
