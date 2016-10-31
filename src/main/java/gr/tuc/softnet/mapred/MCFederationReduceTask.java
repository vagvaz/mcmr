package gr.tuc.softnet.mapred;

import com.google.common.collect.Lists;
import gr.tuc.softnet.Utils;
import gr.tuc.softnet.core.PrintUtilities;
import gr.tuc.softnet.engine.MCTask;
import gr.tuc.softnet.engine.TaskConfiguration;
import gr.tuc.softnet.kvs.KVSProxy;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action1;
import rx.observables.BlockingObservable;
import rx.observers.Observers;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by vagvaz on 03/03/16.
 */
public class MCFederationReduceTask<INKEY extends WritableComparable,INVALUE extends Writable,OUTKEY extends WritableComparable, OUTVALUE extends Writable> extends MCTaskBaseImpl implements MCTask{



    Class reducerClass;
    MCReducer<INKEY,INVALUE,OUTKEY, OUTVALUE> reducer;


    org.slf4j.Logger logger = LoggerFactory.getLogger(MCFederationReduceTask.class);
    private Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE>.Context context;
    Subscriber<Map.Entry<INKEY,Iterator<INVALUE>>> subscriber = new Subscriber<Map.Entry<INKEY, Iterator<INVALUE>>>() {

        @Override
        public void onCompleted() {

            inputEnabled = false;
            synchronized (mutex){
                mutex.notifyAll();
            }
        }

        @Override
        public void onError(Throwable e) {

            PrintUtilities.logStackTrace(logger,e.getStackTrace());
            status.setException(e);
            inputEnabled = false;
        }

        @Override
        synchronized public void onNext(Map.Entry<INKEY, Iterator<INVALUE>> inkeyIterableEntry) {
            try {

                reducer.reduce(inkeyIterableEntry.getKey(), Utils.iterable(inkeyIterableEntry.getValue()),reduceContext);
            } catch (IOException e) {
                status.setException(e);
                inputEnabled = false;
                e.printStackTrace();
            } catch (Exception e){
                e.printStackTrace();
                status.setException(e);
                inputEnabled = false;
            }
        }
    };


    public void initialize(TaskConfiguration configuration){
        super.initialize(configuration);
        reducerClass = configuration.getFederationReducerClass();
        reducer = initializeFederationReducer(reducerClass,keyClass,valueClass,outKeyClass,outValueClass);

        BlockingObservable ob = Observable.from(inputStore.iterator()).toBlocking();
        ob.subscribe(subscriber);
        initialized();
    }



    @Override
    public boolean start() {
        return false;
    }


    @Override
    public void run() {
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

    @Override
    synchronized public void finalizeTask() {
        try {
            reducer.cleanup(reduceContext);
            KVSProxy proxy = ((MCReducer.Context)reduceContext).getReduceContext().getOutputProxy();
            proxy.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean enabledInput() {
        return inputEnabled;
    }

}
