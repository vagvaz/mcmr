package gr.tuc.softnet.netty.messages;

import io.netty.buffer.ByteBuf;

import java.io.*;

/**
 * Created by vagvaz on 06/04/16.
 */
public abstract class MCMessage {

  String messageType;
  public MCMessage(String messageType){
    messageType = messageType;
  }
  public String getMessageType() {
    return messageType;
  }

  public void setMessageType(String messageType) {
    this.messageType = messageType;
  }

  public abstract byte[] toBytes();
  public abstract void fromBytes(byte[] bytes);

  protected void appendString(ByteBuf buf,String string) throws UnsupportedEncodingException {
    byte[] bytes = string.getBytes("UTF8");
    buf.writeInt(bytes.length);
    buf.writeBytes(bytes);
  }
  protected String readString(ByteBuf buf) throws UnsupportedEncodingException {
    int size = buf.readInt();
    byte[] bytes = new byte[size];
    buf.readBytes(bytes);
    return  new String(bytes,"UTF8");
  }
}
