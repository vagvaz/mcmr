package gr.tuc.softnet.netty;

import gr.tuc.softnet.netty.messages.MCMessage;

/**
 * Created by vagvaz on 03/07/16.
 */
public class RequestResponseEvent {
  String node;
  MCMessage message;
  Long requestID;
  public RequestResponseEvent(String node, long requestID, MCMessage message){
    this.node = node;
    this.requestID = requestID;
    this.message = message;
  }

  public String getNode() {
    return node;
  }

  public void setNode(String node) {
    this.node = node;
  }

  public MCMessage getMessage() {
    return message;
  }

  public void setMessage(MCMessage message) {
    this.message = message;
  }

  public Long getRequestID() {
    return requestID;
  }

  public void setRequestID(Long requestID) {
    this.requestID = requestID;
  }
}
