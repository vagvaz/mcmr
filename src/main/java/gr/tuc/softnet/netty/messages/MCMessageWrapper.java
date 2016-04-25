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
  }

  public MCMessageWrapper(byte[] bytes, String type, long requestId){
    this.byte_message = bytes;
    this.type = type;
  }

  public long getRequestId() {
    return requestId;
  }

  public void setRequestId(long requestId) {
    this.requestId = requestId;
  }

  public MCMessage getMessage() {
    return message;
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
    out.writeUTF(type);
    out.writeInt(byte_message.length);
    out.write(byte_message);
  }

  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    type = in.readUTF();
    byte_message = new byte[in.readInt()];
    int readBytes = in.read(byte_message);
    if(readBytes != byte_message.length) {
      throw new IOException("Bytes are not ready ");
    }
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
