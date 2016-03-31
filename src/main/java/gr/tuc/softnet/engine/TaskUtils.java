package gr.tuc.softnet.engine;

import gr.tuc.softnet.core.InjectorUtils;
import gr.tuc.softnet.core.MCNodeModule;
import gr.tuc.softnet.mapred.MCFederationReduceTask;
import gr.tuc.softnet.mapred.MCMapTask;
import gr.tuc.softnet.mapred.MCMapper;
import gr.tuc.softnet.mapred.MCReduceLocalTask;

/**
 * Created by vagvaz on 03/03/16.
 */
public class TaskUtils {
    public static MCTask createTask(TaskConfiguration taskConfiguration) {
        MCTask result =null;
        if(taskConfiguration.isMap()){

            result = InjectorUtils.getInjector().getInstance(MCMapTask.class);
        }else if(taskConfiguration.isLocalReduce()){
            result = InjectorUtils.getInjector().getInstance(MCReduceLocalTask.class);
        }else if(taskConfiguration.isFederationReduce()){
            result = InjectorUtils.getInjector().getInstance(MCFederationReduceTask.class);
        }
        result.initialize(taskConfiguration);
        return result;
    }
}
