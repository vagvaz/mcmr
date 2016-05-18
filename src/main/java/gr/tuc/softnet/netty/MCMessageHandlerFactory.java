package gr.tuc.softnet.netty;

import gr.tuc.softnet.netty.handlers.EngineHandler;
import gr.tuc.softnet.netty.handlers.KVSHandler;
import gr.tuc.softnet.netty.handlers.NodeHandler;

/**
 * Created by vagvaz on 18/05/16.
 */
public class MCMessageHandlerFactory {
  public static MCMessageHandler getHandler(String type) {
    if(type.startsWith("KVS")){
      return new KVSHandler();
    } else if(type.startsWith("ENG")){
      return new EngineHandler();
    } else if(type.startsWith("NOD")){
      return new NodeHandler();
    }else{
      return null;
    }
  }
}
