package gr.tuc.softnet.netty.messages;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

/**
 * Created by vagvaz on 21/04/16.
 */
public class KVSGetResponse<K extends WritableComparable, V extends Writable> extends MCMessage {
  public static final String TYPE = "KVSGetResponse";
  Class<K> keyClass;
  Class<V> valueClass;
  K key;
  V value;
  private static Configuration conf = new Configuration();

  public  KVSGetResponse(){
    super(TYPE);
  }

  public KVSGetResponse(K key, Class<K> keyClass, V value, Class<V> valueClass){
    super(TYPE);
    this.key = key;
    this.keyClass = keyClass;
    this.value = value;
    this.valueClass = valueClass;
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
    try {
      key.write(output);
      value.write(output);
    } catch (IOException e) {
      e.printStackTrace();
    }
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
      keyClass = (Class<K>) ios.readObject();
      valueClass = (Class<V>) ios.readObject();
      key = ReflectionUtils.newInstance(keyClass,conf);
      ByteArrayDataInput dataInput =  ByteStreams.newDataInput(bios);
      key.readFields(dataInput);
      value = ReflectionUtils.newInstance(valueClass,conf);
      value.readFields(dataInput);
      ios.close();
      bios.close();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public V getValue() {
    return value;
  }
}
