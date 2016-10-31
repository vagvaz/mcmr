package gr.tuc.softnet.netty.handlers;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import gr.tuc.softnet.core.InjectorUtils;
import gr.tuc.softnet.kvs.IntermediateKeyValueStore;
import gr.tuc.softnet.kvs.KVSConfiguration;
import gr.tuc.softnet.kvs.KVSManager;
import gr.tuc.softnet.kvs.KeyValueStore;
import gr.tuc.softnet.netty.MCMessageHandler;
import gr.tuc.softnet.netty.RequestResponseEvent;
import gr.tuc.softnet.netty.messages.*;
import io.netty.channel.Channel;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by vagvaz on 18/05/16.
 */
public class KVSHandler extends MCMessageHandler {
  KVSManager kvsManager;
  private Logger log = LoggerFactory.getLogger(KVSHandler.class);

  public KVSHandler() {
    super();
    kvsManager = InjectorUtils.getInjector().getInstance(KVSManager.class);
  }

  @Override public MCMessageWrapper process(Channel channel, MCMessageWrapper wrapper) {
    MCMessage result = null;
    if (wrapper.getType().equals(KVSDescriptionRequest.TYPE)) {
      KVSDescriptionRequest request = (KVSDescriptionRequest) wrapper.getMessage();

      KVSConfiguration kvsConfiguration = kvsManager.getKVSConfiguration(request.getName());
      if (kvsConfiguration == null) {
        kvsConfiguration = new KVSConfiguration();
      }
      result = new KVSDescriptionResponse(kvsConfiguration);
    } else if (wrapper.getType().equals(KVSDescriptionResponse.TYPE)) {
      RequestResponseEvent event =
        new RequestResponseEvent(transport.getNodeName(channel), -wrapper.getRequestId(),
          wrapper.getMessage());
      transport.getEventBus().post(event);
    } else if (wrapper.getType().equals(KVSCreate.TYPE)) {
      KVSCreate request = (KVSCreate) wrapper.getMessage();
      KVSConfiguration configuration = request.getConfiguration();
      KVSConfiguration kvsConfiguration = kvsManager.bootstrapKVS(configuration);
      result = new KVSDescriptionResponse(kvsConfiguration);
    } else if (wrapper.getType().equals(RemoteKVSCreate.TYPE)) {
      RemoteKVSCreate request = (RemoteKVSCreate) wrapper.getMessage();
      KVSConfiguration configuraiton = request.getConfiguration();
      KVSConfiguration kvsConfiguration =
        kvsManager.remoteCreateKVS(configuraiton.getName(), configuraiton);
      result = new KVSDescriptionResponse(kvsConfiguration);
    } else if (wrapper.getType().equals(KVSBatchPut.TYPE)) {
      KVSBatchPut batchput = (KVSBatchPut) wrapper.getMessage();
      ByteArrayDataInput input = ByteStreams.newDataInput(batchput.getData());
      String kvsName = batchput.getKvsName();
      KeyValueStore store = kvsManager.getKVS(kvsName);
      boolean intemediate_destination = store instanceof IntermediateKeyValueStore;
      if (store == null) {
        log.error("BatchPut Received from " + transport.getNodeName(channel) + " for " + kvsName
          + " that does not exist");

      } else {
        Class<? extends WritableComparable> keyClass = batchput.getKeyClass();
        Class<? extends Writable> valueClass = batchput.getValueClass();
        int numberOfValues = input.readInt();
        int byteSize = input.readInt();
        byte[] data = new byte[byteSize];
        input.readFully(data);
        ByteArrayDataInput dataInput = ByteStreams.newDataInput(data);

        for (int i = 0; i < numberOfValues; i++) {
          WritableComparable key = ReflectionUtils.newInstance(keyClass, null);
          Writable value = ReflectionUtils.newInstance(valueClass, null);
          try {
            key.readFields(dataInput);
            value.readFields(dataInput);
            if (intemediate_destination) {
              ((IntermediateKeyValueStore) store).append(key, value);
            } else {
              store.put(key, value);
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
      result = new EmptyKVSResponse();
    } else if (wrapper.getType().equals(KVSContains.TYPE)) {
      KVSContains request = (KVSContains) wrapper.getMessage();
      String kvsName = request.getKvsname();
      WritableComparable key = request.getKey();
      KeyValueStore store = kvsManager.getKVS(kvsName);
      if (store == null) {
        log.error("BatchPut Received from " + transport.getNodeName(channel) + " for " + kvsName
          + " that does not exist");
      } else {
        Writable value = (Writable) store.get(key);

        if (value == null) {
          result = new KVSContainsResponse(false);
        } else {
          result = new KVSContainsResponse(true);
        }
      }

    } else if (wrapper.getType().equals(KVSDestroy.TYPE)) {
      KVSDestroy destroy = (KVSDestroy) wrapper.getMCMessage();
      kvsManager.destroyKVS(destroy.getKvsName());
    } else if (wrapper.getType().equals(KVSGet.TYPE)) {
      KVSGet request = (KVSGet) wrapper.getMessage();
      String kvsName = request.getName();
      WritableComparable key = request.getKey();
      KeyValueStore store = kvsManager.getKVS(kvsName);
      if (store == null) {
        log.error("KVSGet Received from " + transport.getNodeName(channel) + " for " + kvsName
          + " that does not exist");
      } else {
        Writable value = (Writable) store.get(key);

        if (value == null) {
          result = new KVSGetResponse(key, kvsManager.getKeyClass(store), value,
            kvsManager.getValueClass(store));
        } else {
          result = new KVSGetResponse(key, kvsManager.getKeyClass(store), NullWritable.get(),
            NullWritable.class);
        }
      }
    } else if (wrapper.getType().equals(KVSPut.TYPE)) {
      KVSPut put = (KVSPut) wrapper.getMessage();
      String kvsName = put.getKvsName();
      WritableComparable key = put.getKey();
      Writable value = put.getValue();
      KeyValueStore store = kvsManager.getKVS(kvsName);
      if (store == null) {
        log.error("KVSGet Received from " + transport.getNodeName(channel) + " for " + kvsName
          + " that does not exist");
      } else {
        try {
          store.put(key, value);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    } else if (wrapper.getType().equals(EmptyKVSResponse.TYPE)) {
      RequestResponseEvent event =
        new RequestResponseEvent(transport.getNodeName(channel), -wrapper.getRequestId(),
          wrapper.getMessage());
      transport.getEventBus().post(event);
    } else {
      log.error(
        "Unknown KVS message " + wrapper.getType() + " from " + transport.getNodeName(channel));
    }
    if (result != null) {
      return new MCMessageWrapper(result, -wrapper.getRequestId());
    }
    return null;
  }
}
