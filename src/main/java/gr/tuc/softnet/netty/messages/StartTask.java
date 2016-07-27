package gr.tuc.softnet.netty.messages;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import gr.tuc.softnet.core.StringConstants;
import gr.tuc.softnet.engine.TaskConfiguration;

import java.io.*;

/**
 * Created by vagvaz on 22/04/16.
 */
public class StartTask extends MCMessage {
  public static final String TYPE = StringConstants.START_TASK;
  TaskConfiguration conf;
  public StartTask(){
    super(TYPE);
  }
  public StartTask(TaskConfiguration taskConfiguration){
    super(TYPE);
    this.conf = taskConfiguration;
  }

  public TaskConfiguration getConf() {
    return conf;
  }

  public void setConf(TaskConfiguration conf) {
    this.conf = conf;
  }

  @Override public byte[] toBytes() {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteArrayDataOutput output = null;
    try {
      ObjectOutputStream stream = new ObjectOutputStream(bos);
      stream.writeObject(conf);
      output = ByteStreams.newDataOutput(bos);
      bos.close();
      stream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return output.toByteArray();
  }

  @Override public void fromBytes(byte[] bytes) {
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    try {
      ObjectInputStream ois = new ObjectInputStream(bis);
      conf = (TaskConfiguration) ois.readObject();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }
}
