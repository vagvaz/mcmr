package gr.tuc.softnet.netty.messages;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import gr.tuc.softnet.core.StringConstants;
import io.netty.buffer.ByteBuf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;

/**
 * Created by vagvaz on 06/04/16.
 */
public class NoMoreInputMessage extends MCMessage {
  public static final String TYPE = StringConstants.NOMOREINPUT;
  private  String kvsName;
  private  String taskID;
  private String nodeID;

  public NoMoreInputMessage(){
    super(TYPE);
  }

  public NoMoreInputMessage(String nodeID, String kvsName, String taskID) {
    super(TYPE);
    this.nodeID = nodeID;
    this.kvsName = kvsName;
    this.taskID = taskID;
  }

  public String getKvsName() {
    return kvsName;
  }

  public void setKvsName(String kvsName) {
    this.kvsName = kvsName;
  }

  public String getTaskID() {
    return taskID;
  }

  public void setTaskID(String taskID) {
    this.taskID = taskID;
  }

  public String getNodeID() {
    return nodeID;
  }

  public void setNodeID(String nodeID) {
    this.nodeID = nodeID;
  }

  @Override public byte[] toBytes() {
    ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput();
    dataOutput.writeUTF(nodeID);
    dataOutput.writeUTF(kvsName);
    dataOutput.writeUTF(taskID);
    return dataOutput.toByteArray();
  }

  @Override public void fromBytes(byte[] bytes) {
    ByteArrayDataInput input = ByteStreams.newDataInput(bytes);
    nodeID = input.readUTF();
    kvsName = input.readUTF();
    taskID = input.readUTF();
  }
}
