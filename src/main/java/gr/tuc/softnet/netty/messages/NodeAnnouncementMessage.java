package gr.tuc.softnet.netty.messages;

import java.io.UnsupportedEncodingException;

/**
 * Created by vagvaz on 03/07/16.
 */
public class NodeAnnouncementMessage extends MCMessage {
  public static final String TYPE = "NODAnouncement";
  String nodeName;
  public NodeAnnouncementMessage(String nodeName){
    super(TYPE);
    this.nodeName = nodeName;
  }

  public NodeAnnouncementMessage() {
    super(TYPE);
  }

  @Override public byte[] toBytes() {
    try {
      return nodeName.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      return nodeName.getBytes();
    }
  }

  @Override public void fromBytes(byte[] bytes) {
    try {
      nodeName = new String(bytes,"UTF-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      nodeName = new String(bytes);
    }
  }

  public String getName() {
    return nodeName;
  }
}
