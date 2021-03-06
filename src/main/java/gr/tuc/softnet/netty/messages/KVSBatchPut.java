package gr.tuc.softnet.netty.messages;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import gr.tuc.softnet.core.StringConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

/**
 * Created by vagvaz on 21/04/16.
 */
public class KVSBatchPut extends MCMessage {
  public static final String TYPE = StringConstants.KVS_BATCH_PUT;
  Class<? extends WritableComparable> keyClass;
  Class<? extends Writable> valueClass;
  byte[] data;
  String kvsName;

  public  KVSBatchPut(){
    super(TYPE);
  }

  public KVSBatchPut(Class<? extends WritableComparable> keyClass,  Class<? extends Writable> valueClass, byte[] data, String kvsName){
    super(TYPE);
    this.keyClass = keyClass;
    this.data = data;
    this.valueClass = valueClass;
    this.kvsName = kvsName;
  }

  @Override public byte[] toBytes() {
    ObjectOutputStream oos = null;
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      oos =  new ObjectOutputStream(bos);
      oos.writeObject(keyClass);
      oos.writeObject(valueClass);
    } catch (IOException e) {
      e.printStackTrace();
    }
    ByteArrayDataOutput output = ByteStreams.newDataOutput(bos);

      output.writeInt(data.length);
      output.write(data);
      output.writeUTF(kvsName);

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
      keyClass = (Class<? extends WritableComparable>) ios.readObject();
      valueClass = (Class<? extends Writable>) ios.readObject();

      ByteArrayDataInput dataInput =  ByteStreams.newDataInput(bios);
      int size = dataInput.readInt();
      data = new byte[size];
      dataInput.readFully(data);
      kvsName = dataInput.readUTF();

      ios.close();
      bios.close();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public Class<? extends WritableComparable> getKeyClass() {
    return keyClass;
  }

  public void setKeyClass(Class<? extends WritableComparable> keyClass) {
    this.keyClass = keyClass;
  }

  public Class<? extends Writable> getValueClass() {
    return valueClass;
  }

  public void setValueClass(Class<? extends Writable> valueClass) {
    this.valueClass = valueClass;
  }

  public byte[] getData() {
    return data;
  }

  public void setData(byte[] data) {
    this.data = data;
  }

  public String getKvsName() {
    return kvsName;
  }

  public void setKvsName(String kvsName) {
    this.kvsName = kvsName;
  }
}
