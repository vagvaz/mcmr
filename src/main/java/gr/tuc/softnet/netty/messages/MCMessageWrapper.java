package gr.tuc.softnet.netty.messages;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;

/**
 * Created by vagvaz on 13/04/16.
 */
public class MCMessageWrapper implements Serializable {
  MCMessage message;
  byte[] byte_message;
  String type;
  long requestId;
  public MCMessageWrapper(MCMessage message, long requestId){
    this.message = message;
    this.type = message.getMessageType();
    this.requestId = requestId;
  }

  public MCMessageWrapper(byte[] bytes, String type, long requestId){
    this.byte_message = bytes;
    this.type = type;
    this.requestId = requestId;
  }

  public long getRequestId() {
    return requestId;
  }

  public void setRequestId(long requestId) {
    this.requestId = requestId;
  }

  public MCMessage getMessage() {
    return getMCMessage();
  }

  public void setMessage(MCMessage message) {
    this.message = message;
  }

  public byte[] getByte_message() {
    return byte_message;
  }

  public void setByte_message(byte[] byte_message) {
    this.byte_message = byte_message;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    if(byte_message == null){
      byte_message = message.toBytes();
    }
    out.writeInt(byte_message.length);
    out.write(byte_message);
    out.writeObject(this.getType());
    out.writeLong(requestId);
  }

  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    byte_message = new byte[in.readInt()];
    in.readFully(byte_message);
//    if(readBytes != byte_message.length) {
//      System.err.println("Bytes are not ready " + readBytes + " instead of " + byte_message.length);
//    }
    type = (String) in.readObject();
    requestId = in.readLong();
  }

  private void readObjectNoData() throws ObjectStreamException {
    message = null;
    byte_message = new byte[0];
    type = "";
  }

  public MCMessage getMCMessage(){
    if(message == null){
      if(byte_message != null){
        message = MCMessageFactory.getMessage(type);
        message.fromBytes(byte_message);
      }
    }
    return message;
  }

  public byte[] bytesSize() {
    if(byte_message == null){
      byte_message = message.toBytes();
    }
    return byte_message;
  }
}
