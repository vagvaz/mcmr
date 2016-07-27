package gr.tuc.softnet.netty.messages;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import gr.tuc.softnet.core.StringConstants;
import gr.tuc.softnet.engine.TaskConfiguration;

import java.io.*;

/**
 * Created by vagvaz on 17/05/16.
 */
public class TaskCompleted extends MCMessage {
  public static final String TYPE = StringConstants.TASK_COMPLETED;

  private  TaskConfiguration task;

  public TaskCompleted(TaskConfiguration task) {
    super(TYPE);
    this.task = task;
  }

  public TaskCompleted() {
    super(TYPE);
  }
  public TaskConfiguration getTask() {
    return task;
  }

  public void setTask(TaskConfiguration task) {
    this.task = task;
  }

  @Override public byte[] toBytes() {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteArrayDataOutput output = null;
    try {
      ObjectOutputStream stream = new ObjectOutputStream(bos);
      stream.writeObject(task);
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
      task = (TaskConfiguration) ois.readObject();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }


}
