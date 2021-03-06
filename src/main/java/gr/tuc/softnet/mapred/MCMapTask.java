package gr.tuc.softnet.mapred;

import gr.tuc.softnet.core.PrintUtilities;
import gr.tuc.softnet.engine.MCTask;
import gr.tuc.softnet.engine.TaskConfiguration;
import gr.tuc.softnet.kvs.KVSProxy;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.observables.BlockingObservable;

import java.io.IOException;
import java.util.Map;

/**
 * Created by vagvaz on 03/03/16.
 */
public class MCMapTask<INKEY extends WritableComparable, INVALUE extends Writable, OUTKEY extends WritableComparable, OUTVALUE extends Writable> extends MCTaskBaseImpl implements MCTask {
  Class<?> mapperClass;
  MCMapper<INKEY, INVALUE, OUTKEY, OUTVALUE> mapper;


  org.slf4j.Logger logger = LoggerFactory.getLogger(MCFederationReduceTask.class);

  public MCMapTask() {

  }


  Subscriber<Map.Entry<INKEY, INVALUE>> subscriber = new Subscriber<Map.Entry<INKEY, INVALUE>>() {
    @Override public void onCompleted() {
      inputEnabled = false;
      synchronized (mutex){
        mutex.notifyAll();
      }
    }

    @Override public void onError(Throwable e) {

      PrintUtilities.logStackTrace(logger, e.getStackTrace());
      status.setException(e);
      synchronized (mutex) {
        inputEnabled = false;
        mutex.notifyAll();
      }
    }

    @Override synchronized public void onNext(Map.Entry<INKEY, INVALUE> entry) {
      try {
        mapper.map(entry.getKey(), entry.getValue(), mapContext);
      } catch (IOException e) {
        Observable.create(inputStore).subscribe(subscriber);
        status.setException(e);
        inputEnabled = false;
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
        status.setException(e);
        inputEnabled = false;
      }
    }
  };


  public void initialize(TaskConfiguration configuration) {
    super.initialize(configuration);
    mapperClass = configuration.getMapperClass();
    mapper = initializeMapper(mapperClass, keyClass, valueClass, outKeyClass, outValueClass);
    BlockingObservable ob = Observable.from(inputStore.iterator()).toBlocking();
//    BlockingObservable ob = Observable.create(inputStore).toBlocking();
    ob.subscribe(subscriber);
    initialized();
  }

  @Override public boolean start() {
    return false;
  }


  @Override public void run() {
    synchronized (mutex) {
      while (enabledInput()) {
        try {
          mutex.wait();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    finalizeTask();
    for (Object ob : subscribers) {
      Subscriber<? super MCTask> subscriber = (Subscriber<? super MCTask>) ob;
      subscriber.onNext(this);
      subscriber.onCompleted();
    }

  }

  @Override public void finalizeTask() {
    try {
      mapper.cleanup(mapContext);
      KVSProxy proxy = ((MCMapper.Context)mapContext).getMapContext().getOutputProxy();
      proxy.flush();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Override public boolean enabledInput() {
    return inputEnabled;
  }

}
