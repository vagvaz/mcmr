package gr.tuc.softnet.kvs;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import gr.tuc.softnet.core.MCConfiguration;
import gr.tuc.softnet.netty.MCDataTransport;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Created by vagvaz on 21/04/16.
 */
public class MCDataBufferImpl implements MCDataBuffer {

  private String nodeName;
  Class<? extends WritableComparable> keyClass;
  Class<? extends Writable> valueClass;
  ByteArrayDataOutput output;
  ByteArrayOutputStream bos;
  volatile Object mutex = new Object();
  private int threshold =  1000;
  String threshold_type = "bytes";
  int currentSize = 0;
  MCDataTransport dataTransport;
  String kvsName;

  public MCDataBufferImpl(KVSConfiguration kvsConfiguration, MCDataTransport dataTransport, String nodeName, MCConfiguration configuration) {
    bos = new ByteArrayOutputStream();
    output = ByteStreams.newDataOutput(bos);
    keyClass = (Class<? extends WritableComparable>) kvsConfiguration.getKeyClass();
    valueClass = (Class<? extends Writable>) kvsConfiguration.getValueClass();
    threshold = configuration.conf().getInt("kvs.buffer_threshold",threshold);
    threshold_type = configuration.conf().getString("kvs.buffer_threshold_type",threshold_type);
    this.dataTransport = dataTransport;
    this.nodeName = nodeName;
    kvsName = kvsConfiguration.getName();
  }

  @Override public boolean append(WritableComparable key, Writable value) {
    try {
      synchronized (mutex) {
        key.write(output);
        value.write(output);
      }
      if (threshold_type.startsWith("bytes")) {
        if (bos.size() > threshold) {
          return true;
        } else {
          currentSize++;
          if (currentSize >= threshold) {
            return true;
          }
        }
      }
    }catch(IOException e){
        e.printStackTrace();
      }
    return false;
  }

  @Override public void flush(boolean b) {
    ByteArrayDataOutput outputTmp;
    int size = 0;
    synchronized (mutex) {


      try {
        bos.close();
        dataTransport.batchSend(nodeName, kvsName,toBytes(), keyClass, valueClass,b);
        currentSize = 0;
        bos = new ByteArrayOutputStream();
        output = ByteStreams.newDataOutput(bos);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

  }

  @Override public void clear() {
    ByteArrayDataOutput outputTmp;
    int size = 0;
    synchronized (mutex) {
      outputTmp = output;
      size = currentSize;
      currentSize = 0;
      try {
        bos.close();
        bos = new ByteArrayOutputStream();
        output = ByteStreams.newDataOutput(bos);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Override public byte[] toBytes() {
    synchronized (mutex) {
      byte[] bytes = output.toByteArray();
      ByteArrayDataOutput tmp = ByteStreams.newDataOutput();
      tmp.writeInt(currentSize);
      tmp.writeInt(bytes.length);
      tmp.write(bytes);
      return tmp.toByteArray();
    }
  }

  @Override public byte[] getBytesAndClear() {
    byte[] result = toBytes();
    clear();
    return result;
  }
}
