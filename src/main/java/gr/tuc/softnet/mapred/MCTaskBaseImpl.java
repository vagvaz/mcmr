package gr.tuc.softnet.mapred;

import com.google.inject.Inject;
import gr.tuc.softnet.core.ConfigurationUtilities;
import gr.tuc.softnet.core.MCConfiguration;
import gr.tuc.softnet.engine.MCTask;
import gr.tuc.softnet.engine.TaskConfiguration;
import gr.tuc.softnet.engine.TaskStatus;
import gr.tuc.softnet.kvs.KVSManager;
import gr.tuc.softnet.kvs.KVSProxy;
import gr.tuc.softnet.kvs.KeyValueStore;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by vagvaz on 08/03/16.
 */
public abstract class MCTaskBaseImpl<INKEY extends WritableComparable, INVALUE extends Writable, OUTKEY extends WritableComparable, OUTVALUE extends Writable>
  implements MCTask, Observable.OnSubscribe<MCTask> {
  @Override public void call(Subscriber<? super MCTask> subscriber) {
    subscribers.add(subscriber);
  }

  List<Subscriber<? super MCTask>> subscribers;
  TaskConfiguration configuration;
  MCTaskContext<INKEY, INVALUE, OUTKEY, OUTVALUE> taskContext;
  volatile Object mutex = new Object();
  boolean isCompleted = true;
  KVSProxy outputProxy;
  KeyValueStore inputStore;
  @Inject KVSManager kvsManager;
  TaskStatus status;
  boolean inputEnabled;
  Class<INKEY> keyClass;
  Class<INVALUE> valueClass;
  Class<OUTKEY> outKeyClass;
  Class<OUTVALUE> outValueClass;
  Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE>.Context mapContext;
  Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE>.Context reduceContext;
  Logger logger = LoggerFactory.getLogger(this.getClass());

  @Override public boolean start() {

    return false;
  }

  @Override public void initialize(TaskConfiguration configuration) {
    this.configuration = configuration;
    taskContext = new MCTaskContext<INKEY, INVALUE, OUTKEY, OUTVALUE>(configuration, kvsManager);
    keyClass = (Class<INKEY>) configuration.getKeyClass();
    valueClass = (Class<INVALUE>) configuration.getValueClass();
    outKeyClass = (Class<OUTKEY>) configuration.getOutKeyClass();
    outValueClass = (Class<OUTVALUE>) configuration.getOutValueClass();
    status = new TaskStatus(configuration);
    subscribers = new ArrayList<>();
    inputStore = kvsManager.getKVS(configuration.getInput());
    outputProxy = (KVSProxy) kvsManager.getKVSProxy(configuration.getOutput());
  }

  @Override public TaskStatus getStatus() {
    return status;
  }

  @Override public boolean cancel() {

    inputEnabled = false;
    complete(TaskState.KILLED);
    return true;
  }

  @Override public void waitForCompletion() {
    synchronized (mutex) {
      if (status.getState().equals(TaskState.RUNNING)) {
        try {
          mutex.wait(10000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  @Override public String getCoordinator() {
    return configuration.getCoordinator();
  }

  @Override public TaskConfiguration getTaskConfiguration() {
    return configuration;
  }

  @Override public void finalizeTask() {
    subscribers.clear();
  }


  @Override public void complete(TaskState state) {
    finalizeTask();
    for (Subscriber<? super MCTask> taskSubscriber : subscribers) {
      if (state.equals(TaskState.SUCCESSFUL)) {
        taskSubscriber.onNext(this);
        taskSubscriber.onCompleted();
      } else {
        taskSubscriber.onError(status.getException());
        taskSubscriber.onCompleted();
      }
    }
    synchronized (mutex) {
      mutex.notifyAll();
    }
  }

  @Override public String getInput() {
    return configuration.getInput();
  }

  @Override public String getOutput() {
    return configuration.getOutput();
  }

  @Override public String getID() {
    return configuration.getID();
  }

  @Override public MCConfiguration getConfiguration() {
    return null;
  }

  @Override public void run() {

  }


  public MCReducer<INKEY, INVALUE, OUTKEY, OUTVALUE> initializeFederationReducer(Class reducerClass,
    Class keyClass, Class valueClass, Class outKeyClass, Class outValueClass) {
    MCReducer<INKEY, INVALUE, OUTKEY, OUTVALUE> result =       null; //TODO read from class loader initialize install
    try {
      if (reducerClass.getCanonicalName() != null && !reducerClass.getCanonicalName().equals("")) {
        try {
          //                    Class<?> mapperClass = Class.forName(mapperClassName, true, classLoader);
          Constructor<?> con = reducerClass.getConstructor();
          result = (MCReducer<INKEY, INVALUE, OUTKEY, OUTVALUE>) con.newInstance();
          //        mapper.initialize(pluginConfig, imanager);
          //                } catch (ClassNotFoundException e) {
          //                    e.printStackTrace();
          result.setup(result.getReducerContext(taskContext));
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException | InstantiationException e) {
          e.printStackTrace();
        }
      } else {
        logger.error("Could not find the name for " + reducerClass.getCanonicalName());
      }

    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
    return result;
  }

  public MCReducer<INKEY, INVALUE, OUTKEY, OUTVALUE> initializeLocalReducer(Class reducerClass,
    Class keyClass, Class valueClass, Class outKeyClass, Class outValueClass) {
    MCReducer<INKEY, INVALUE, OUTKEY, OUTVALUE> result =       null; //TODO read from class loader initialize install
    try {
      if (reducerClass.getCanonicalName() != null && !reducerClass.getCanonicalName().equals("")) {
        try {
          //                    Class<?> mapperClass = Class.forName(mapperClassName, true, classLoader);
          Constructor<?> con = reducerClass.getConstructor();
          result = (MCReducer<INKEY, INVALUE, OUTKEY, OUTVALUE>) con.newInstance();
          //        mapper.initialize(pluginConfig, imanager);
          //                } catch (ClassNotFoundException e) {
          //                    e.printStackTrace();
          result.setup(result.getReducerContext(taskContext));
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException | InstantiationException e) {
          e.printStackTrace();
        }
      } else {
        logger.error("Could not find the name for " + reducerClass.getCanonicalName());
      }

    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
    return result;
  }

  public MCMapper<INKEY, INVALUE, OUTKEY, OUTVALUE> initializeMapper(Class mapperClass,
    Class keyClass, Class valueClass, Class outKeyClass, Class outValueClass) {
    MCMapper<INKEY, INVALUE, OUTKEY, OUTVALUE> result =
      null; //TODOread from class loader initialize install
    try {
      ClassLoader classLoader = this.getClass().getClassLoader();
      try {
        if (taskContext.getJar() != null) {
          classLoader = ConfigurationUtilities.getClassLoaderFor(taskContext.getJar());
        }
      } catch (URISyntaxException e) {
        e.printStackTrace();
      }


      //    ConfigurationUtilities.addToClassPath(jarFileName);
      //      .addToClassPath(System.getProperty("java.io.tmpdir") + "/leads/plugins/" + plugName
      //                        + ".jar");

      //    byte[] config = (byte[]) cache.get(plugName + ":conf");


      //    String className = (String) cache.get(plugName + ":className");
      if (mapperClass.getCanonicalName() != null && !mapperClass.getCanonicalName().equals("")) {
        try {
          //                    Class<?> mapperClass = Class.forName(mapperClassName, true, classLoader);
          Constructor<?> con = mapperClass.getConstructor();
          result = (MCMapper) con.newInstance();
          //        mapper.initialize(pluginConfig, imanager);
          //                } catch (ClassNotFoundException e) {
          //                    e.printStackTrace();
        } catch (NoSuchMethodException e) {
          e.printStackTrace();
        } catch (InvocationTargetException e) {
          e.printStackTrace();
        } catch (InstantiationException e) {
          e.printStackTrace();
        } catch (IllegalAccessException e) {
          e.printStackTrace();
        }
      } else {
        logger.error("Could not find the name for " + mapperClass.getCanonicalName());
      }
      mapContext = result.getMapContext(taskContext);
      result.setup(mapContext);
      return result;
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return result;
  }
}
