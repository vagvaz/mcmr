package gr.tuc.softnet.mapred;

import gr.tuc.softnet.core.MCConfiguration;
import gr.tuc.softnet.core.PrintUtilities;
import gr.tuc.softnet.engine.MCTask;
import gr.tuc.softnet.engine.TaskConfiguration;
import gr.tuc.softnet.engine.TaskStatus;
import gr.tuc.softnet.kvs.KVSProxy;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;

import java.io.IOException;
import java.util.Map;

/**
 * Created by vagvaz on 03/03/16.
 */
public class MCReduceLocalTask<INKEY extends WritableComparable,INVALUE extends Writable,OUTKEY extends WritableComparable, OUTVALUE extends Writable> extends MCTaskBaseImpl implements MCTask{
    Class reducerClass;
    MCReducer<INKEY,INVALUE,OUTKEY, OUTVALUE> reducer;


    org.slf4j.Logger logger = LoggerFactory.getLogger(MCFederationReduceTask.class);
    private Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE>.Context context;
    Subscriber<Map.Entry<INKEY,Iterable<INVALUE>>> subscriber = new Subscriber<Map.Entry<INKEY, Iterable<INVALUE>>>() {
        @Override
        public void onCompleted() {
            inputEnabled = false;
        }

        @Override
        public void onError(Throwable e) {

            PrintUtilities.logStackTrace(logger,e.getStackTrace());
            status.setException(e);
            inputEnabled = false;
        }

        @Override
        public void onNext(Map.Entry<INKEY, Iterable<INVALUE>> inkeyIterableEntry) {
            try {
                reducer.reduce(inkeyIterableEntry.getKey(),inkeyIterableEntry.getValue(),reduceContext);
            } catch (IOException e) {
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


    public void initialize(TaskConfiguration configuration){
        super.initialize(configuration);
        reducerClass = configuration.getLocalReducerClass();
        reducer = initializeLocalReducer(reducerClass,keyClass,valueClass,outKeyClass,outValueClass);
        Observable.create(inputStore).subscribe(subscriber);
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
        for(Object ob : subscribers){
            Subscriber<? super MCTask> subscriber = (Subscriber<? super MCTask>) ob;
            subscriber.onNext(this);
            subscriber.onCompleted();
        }

    }

    @Override
    public void finalizeTask() {
        try {
            reducer.cleanup(context);
            KVSProxy proxy = ((MCReducer.Context)context).getReduceContext().getOutputProxy();
            proxy.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean enabledInput() {
        return false;
    }

}

