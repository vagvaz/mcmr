package gr.tuc.softnet.mapred;

import gr.tuc.softnet.core.PrintUtilities;
import gr.tuc.softnet.engine.MCTask;
import gr.tuc.softnet.engine.TaskConfiguration;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;

import java.io.IOException;
import java.util.Map;

/**
 * Created by vagvaz on 03/03/16.
 */
public class MCFederationReduceTask<INKEY,INVALUE,OUTKEY, OUTVALUE> extends MCTaskBaseImpl implements MCTask{



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
                reducer.reduce(inkeyIterableEntry.getKey(),inkeyIterableEntry.getValue(),context);
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

        reducerClass = configuration.getFederationReducerClass();
        reducer = initializeFederationReducer(reducerClass,keyClass,valueClass,outKeyClass,outValueClass);
        Observable.create(inputStore).subscribe(subscriber);
    }



    @Override
    public boolean start() {
        return false;
    }


    @Override
    public void run() {
        while(enabledInput()){

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
