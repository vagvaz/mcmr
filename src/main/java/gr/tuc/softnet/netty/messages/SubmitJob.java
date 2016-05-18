package gr.tuc.softnet.netty.messages;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import gr.tuc.softnet.core.StringConstants;
import gr.tuc.softnet.engine.JobConfiguration;
import gr.tuc.softnet.kvs.KVSConfiguration;

import java.io.*;

/**
 * Created by vagvaz on 17/05/16.
 */
public class SubmitJob extends MCMessage {
  public static final String TYPE = StringConstants.SUBMIT_JOB;
  String client;
  JobConfiguration configuration;
  public SubmitJob(String client,JobConfiguration configuration) {
    super(TYPE);
    this.client = client;
    this.configuration = configuration;
  }

  @Override public byte[] toBytes() {
    ObjectOutputStream oos = null;
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      oos =  new ObjectOutputStream(bos);
      oos.writeObject(client);
      oos.writeObject(configuration);
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
      client = (String) ios.readObject();
      configuration = (JobConfiguration) ios.readObject();
      ios.close();
      bios.close();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public String getClient() {
    return client;
  }

  public void setClient(String client) {
    this.client = client;
  }

  public JobConfiguration getConfiguration() {
    return configuration;
  }

  public void setConfiguration(JobConfiguration configuration) {
    this.configuration = configuration;
  }
}
