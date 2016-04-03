package gr.tuc.softnet.engine;

import com.google.inject.Inject;
import gr.tuc.softnet.core.ConfStringConstants;
import gr.tuc.softnet.core.MCConfiguration;
import gr.tuc.softnet.netty.MCDataTransport;
import org.apache.commons.collections.map.HashedMap;
import rx.Observable;
import rx.Observer;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by vagvaz on 03/03/16.
 */
public class TaskManagerImpl implements TaskManager, Observer<MCTask> {
    Map<String, MCTask> taskInfo;
    @Inject
    JobManager jobManager;
    @Inject
    MCConfiguration configuration;
    @Inject
    MCDataTransport dataTransport;


    ExecutorService batchExecutor;
    ExecutorService pipeTaskExecutor;

    @Override
    public boolean startTask(TaskConfiguration taskConfiguration) {
        MCTask task = TaskUtils.createTask(taskConfiguration);
        Observable.create((Observable.OnSubscribe<MCTask>)task).subscribe(this);

        taskInfo.put(task.getID(), task);
        if (task.getTaskConfiguration().isBatch()) {
            batchExecutor.submit(task);
        } else {
            pipeTaskExecutor.submit(task);
        }
        return true;
    }

    @Override
    public TaskStatus getTaskStatus(String taskID) {
        MCTask task = taskInfo.get(taskID);
        if (task == null) {
            return null;
        } else {
            return task.getStatus();
        }
    }

    @Override
    public boolean cancelTask(String taskID) {
        MCTask task = taskInfo.get(taskID);
        if (task == null) {
            return false;
        } else {
            return task.cancel();
        }
    }

    @Override
    public void waitForTaskCompletion(String taskID) {
        MCTask task = taskInfo.get(taskID);
        if (task == null) {
            return;
        } else {
            task.waitForCompletion();
        }
    }

    @Override
    public String getID() {
        return configuration.conf().getString(ConfStringConstants.TASK_MANAGER_ID, configuration.getNodeName() + ".taskManager");
    }

    @Override
    public MCConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public void initialize() {
        taskInfo = new HashedMap();
        configuration.conf().setProperty(ConfStringConstants.TASK_MANAGER_ID, configuration.getNodeName() + ".taskManager");
        batchExecutor = new ThreadPoolExecutor(configuration.conf().getInt("engine.processing.batch_threads_min"), configuration.conf().getInt("engine.processing.batch_threads_max"), 2000,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        pipeTaskExecutor = new ThreadPoolExecutor(configuration.conf().getInt("engine.processing.pipeline_threads_min"), configuration.conf().getInt("engine.processing.pipeline_threads_max"), 2000,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>());
    }

    @Override
    public void onCompleted() {

    }

    @Override
    public void onError(Throwable throwable) {

    }

    @Override
    public void onNext(MCTask mcTask) {
        complete(mcTask);
    }

    private void complete(MCTask mcTask) {
        String coordinator = mcTask.getTaskConfiguration().getCoordinator();
        if (coordinator.equals(getConfiguration().getNodeName()) || coordinator.equals(getConfiguration().getURI())) {
            jobManager.taskCompleted(mcTask.getTaskConfiguration().getJobID(), mcTask.getTaskConfiguration().getID());
        } else {
            dataTransport.taskCompleted(mcTask.getTaskConfiguration().getCoordinator(), mcTask.getTaskConfiguration().getID());
        }
    }
}
