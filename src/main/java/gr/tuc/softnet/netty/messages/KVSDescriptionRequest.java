package gr.tuc.softnet.netty.messages;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;

/**
 * Created by vagvaz on 03/07/16.
 */
public class KVSDescriptionRequest extends MCMessage {
  String kvsName;
  public static final String TYPE = "KVSDesciptionRequest";
  public KVSDescriptionRequest(String name){
    super(TYPE);
    this.kvsName = name;
  }

  public KVSDescriptionRequest() {
    super(TYPE);
  }

  @Override public byte[] toBytes() {
    try {
      return kvsName.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      return kvsName.getBytes();
    }
  }

  @Override public void fromBytes(byte[] bytes) {
    try {
      kvsName = new String(bytes,"UTF-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      kvsName = new String(bytes);
    }
  }

  public String getName() {
    return kvsName;
  }
}
