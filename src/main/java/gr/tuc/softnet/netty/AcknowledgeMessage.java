package gr.tuc.softnet.netty;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import gr.tuc.softnet.core.StringConstants;
import gr.tuc.softnet.netty.messages.MCMessage;

import java.io.ByteArrayInputStream;
import java.io.Serializable;

/**
 * Created by vagvaz on 11/25/15.
 */
public class AcknowledgeMessage extends MCMessage {
  private long ackMessageId;
  public final static String TYPE = StringConstants.ACK_MSG;
  public AcknowledgeMessage(long l) {
    super(TYPE);
    this.ackMessageId = l;
  }

  public long getAckMessageId() {
    return ackMessageId;
  }

  public void setAckMessageId(long ackMessageId) {
    this.ackMessageId = ackMessageId;
  }

  @Override public byte[] toBytes() {
    ByteArrayDataOutput output = ByteStreams.newDataOutput();
    output.writeLong(ackMessageId);
    return output.toByteArray();
  }

  @Override public void fromBytes(byte[] bytes) {
    ByteArrayDataInput input = ByteStreams.newDataInput(bytes);
    ackMessageId = input.readLong();
  }
}
