package gr.tuc.softnet.netty.messages;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import gr.tuc.softnet.core.StringConstants;
import gr.tuc.softnet.engine.JobConfiguration;

import java.io.*;

/**
 * Created by vagvaz on 17/05/16.
 */
public class JobCompleted extends MCMessage {
  public static final String TYPE = StringConstants.JOB_COMPLETED;
  private  JobConfiguration conf;

  public JobCompleted(JobConfiguration jobConfiguration){
    super(TYPE);
    this.conf = jobConfiguration;
  }
  public JobCompleted() {
    super(TYPE);
  }

  public JobConfiguration getConf() {
    return conf;
  }

  public void setConf(JobConfiguration conf) {
    this.conf = conf;
  }

  @Override public byte[] toBytes() {
    ObjectOutputStream oos = null;
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      oos =  new ObjectOutputStream(bos);
      oos.writeObject(conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
    ByteArrayDataOutput output = ByteStreams.newDataOutput(bos);

    try {
      oos.close();
      bos.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return output.toByteArray();
  }

  @Override public void fromBytes(byte[] bytes) {
    ObjectInputStream ios = null;
    ByteArrayInputStream bios = new ByteArrayInputStream(bytes);
    try {
      ios = new ObjectInputStream(bios);
      conf = (JobConfiguration) ios.readObject();
      ios.close();
      bios.close();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

}
