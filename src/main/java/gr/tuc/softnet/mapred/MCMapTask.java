package gr.tuc.softnet.mapred;

import gr.tuc.softnet.core.MCConfiguration;
import gr.tuc.softnet.core.PrintUtilities;
import gr.tuc.softnet.engine.MCTask;
import gr.tuc.softnet.engine.TaskConfiguration;
import gr.tuc.softnet.engine.TaskStatus;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;

import java.io.IOException;
import java.util.Map;

/**
 * Created by vagvaz on 03/03/16.
 */
public class MCMapTask<INKEY,INVALUE,OUTKEY, OUTVALUE> extends MCTaskBaseImpl implements MCTask{
    Class<?> mapperClass;
    MCMapper<INKEY,INVALUE,OUTKEY, OUTVALUE> mapper;


    org.slf4j.Logger logger = LoggerFactory.getLogger(MCFederationReduceTask.class);


    private Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE>.Context context;
    Subscriber<Map.Entry<INKEY,INVALUE>> subscriber = new Subscriber<Map.Entry<INKEY, INVALUE>>() {
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
        public void onNext(Map.Entry<INKEY, INVALUE> entry) {
            try {
                mapper.map(entry.getKey(),entry.getValue(),context);
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

        mapperClass = configuration.getMapClass();
        mapper = initializeMapper(mapperClass,keyClass,valueClass,outKeyClass,outValueClass);
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
            mapper.cleanup(context);
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