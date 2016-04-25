package gr.tuc.softnet.netty.messages;

/**
 * Created by vagvaz on 1/04/16.
 */
public class MCMessageFactory {
  public static MCMessage getMessage(String type) {
    switch(type){
      case NoMoreInputMessage.TYPE:
        return new NoMoreInputMessage();
      default:
        return null;
    }
  }
}
