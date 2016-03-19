package gr.tuc.softnet.engine;

import gr.tuc.softnet.core.MCInitializable;
import org.apache.commons.configuration.Configuration;

/**
 * Created by vagvaz on 10/02/16.
 */
public interface TaskManager extends IDable, MCInitializable {
    boolean startTask(TaskConfiguration taskConfiguration);
    TaskStatus getTaskStatus(String taskID);
    boolean cancelTask(String taskID);
    void waitForTaskCompletion(String taskID);
}
