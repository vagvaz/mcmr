package gr.tuc.softnet.mapred;

import com.google.inject.Inject;
import gr.tuc.softnet.core.MCConfiguration;
import gr.tuc.softnet.engine.MCTask;
import gr.tuc.softnet.engine.TaskConfiguration;
import gr.tuc.softnet.engine.TaskStatus;
import gr.tuc.softnet.kvs.KVSManager;
import gr.tuc.softnet.kvs.KVSProxy;
import gr.tuc.softnet.kvs.KeyValueStore;
import rx.Observable;
import rx.Subscriber;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by vagvaz on 08/03/16.
 */
public abstract class MCTaskBaseImpl<INKEY,INVALUE,OUTKEY,OUTVALUE> implements MCTask, Observable.OnSubscribe<MCTask> {
    @Override
    public void call(Subscriber<? super MCTask> subscriber) {
        subscribers.add(subscriber);
    }

    List<Subscriber<? super MCTask>> subscribers;
    TaskConfiguration configuration;
    MCTaskContext<INKEY,INVALUE,OUTKEY,OUTVALUE> taskContext;
    volatile Object mutex = new Object();
    boolean isCompleted = true;
    KVSProxy outputProxy;
    KeyValueStore inputStore;
    @Inject
    KVSManager kvsManager;
    TaskStatus status;
    boolean inputEnabled;
    Class<INKEY> keyClass;
    Class<INVALUE> valueClass;
    Class<OUTKEY> outKeyClass;
    Class<OUTVALUE> outValueClass;
    @Override
    public boolean start() {

      return false;
    }

    @Override
    public void initialize(TaskConfiguration configuration) {
        this.configuration = configuration;
        keyClass = configuration.getKeyClass();
        valueClass = configuration.getValueClass();
        outKeyClass = (Class<OUTKEY>) configuration.getOutKeyClass();
        outValueClass= (Class<OUTVALUE>) configuration.getOutValueClass();
        status = new TaskStatus(this);
        subscribers = new ArrayList<>();
        inputStore = kvsManager.getKVS(configuration.getInput());
        outputProxy = (KVSProxy) kvsManager.getKVSProxy(configuration.getOutput());
    }

    @Override
    public TaskStatus getStatus() {
        return status;
    }

    @Override
    public boolean cancel() {

        inputEnabled = false;
        complete(TaskState.KILLED);
        return  true;
    }

    @Override
    public void waitForCompletion() {
        synchronized (mutex){
            if(status.getState().equals(TaskState.RUNNING)){
                try {
                    mutex.wait(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public String getCoordinator() {
        return configuration.getCoordinator();
    }

    @Override
    public TaskConfiguration getTaskConfiguration() {
        return  configuration;
    }

    @Override
    public void finalizeTask() {
        subscribers.clear();
    }


    @Override
    public void complete(TaskState state) {
        finalizeTask();
        for(Subscriber<? super MCTask> taskSubscriber : subscribers){
            if(state.equals(TaskState.SUCCESSFUL)){
                taskSubscriber.onNext(this);
                taskSubscriber.onCompleted();
            }else{
                taskSubscriber.onError(status.getException());
                taskSubscriber.onCompleted();
            }
        }
        synchronized (mutex){
            mutex.notifyAll();
        }
    }

    @Override
    public String getInput() {
        return configuration.getInput();
    }

    @Override
    public String getOutput() {
        return configuration.getOutput();
    }

    @Override
    public String getID() {
        return configuration.getID();
    }

    @Override
    public MCConfiguration getConfiguration() {
        return null;
    }

    @Override
    public void run() {

    }


    public MCReducer<INKEY, INVALUE, OUTKEY, OUTVALUE> initializeFederationReducer(Class reducerClass, Class keyClass, Class valueClass, Class outKeyClass, Class outValueClass) {
        MCReducer<INKEY,INVALUE,OUTKEY,OUTVALUE> result = null; //TODO read from class loader initialize install
        try {
            result.setup(result.getReducerContext(taskContext));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return result;
    }

    public MCReducer<INKEY, INVALUE, OUTKEY, OUTVALUE> initializeLocalReducer(Class reducerClass, Class keyClass, Class valueClass, Class outKeyClass, Class outValueClass) {
        MCReducer<INKEY,INVALUE,OUTKEY,OUTVALUE> result = null; //TODO read from class loader initialize install
        try {
            result.setup(result.getReducerContext(taskContext));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return result;
    }

    public MCMapper<INKEY, INVALUE, OUTKEY, OUTVALUE> initializeMapper(Class mapperClass, Class keyClass, Class valueClass, Class outKeyClass, Class outValueClass) {
        MCMapper<INKEY,INVALUE,OUTKEY,OUTVALUE> result = null; //TODOread from class loader initialize install
        try {

            result.setup(result.getMapContext(taskContext));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return result;
    }
}
