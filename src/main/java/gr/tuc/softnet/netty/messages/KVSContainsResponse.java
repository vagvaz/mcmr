package gr.tuc.softnet.netty.messages;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

/**
 * Created by vagvaz on 03/07/16.
 */
public class KVSContainsResponse extends MCMessage {
  public static final String TYPE = "KVSContainsResponse";
  boolean result;
  public KVSContainsResponse(boolean b) {
    super(TYPE);
    result = b;
  }

  public boolean isResult() {
    return result;
  }

  public void setResult(boolean result) {
    this.result = result;
  }

  @Override public byte[] toBytes() {
    ByteArrayDataOutput output = ByteStreams.newDataOutput();
    output.writeBoolean(result);
    return output.toByteArray();
  }

  @Override public void fromBytes(byte[] bytes) {
    ByteArrayDataInput input = ByteStreams.newDataInput(bytes);
    result = input.readBoolean();
  }

}
