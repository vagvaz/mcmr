package gr.tuc.softnet.engine;


import gr.tuc.softnet.mapred.TaskState;
import rx.Observable;

/**
 * Created by vagvaz on 10/02/16.
 */
public interface MCTask extends IDable,Runnable {
    boolean start();
    void initialize(TaskConfiguration configuration);
    void initialized();
    TaskStatus getStatus();

    boolean cancel();

    void waitForCompletion();

    String getCoordinator();

    TaskConfiguration getTaskConfiguration();

    void finalizeTask();

    boolean enabledInput();
    void complete(TaskState state);
    String getInput();
    String getOutput();

}
