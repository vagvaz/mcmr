package gr.tuc.softnet.engine;

import com.google.inject.Inject;
import gr.tuc.softnet.core.InjectorUtils;
import gr.tuc.softnet.core.NodeManager;

/**
 * Created by vagvaz on 03/03/16.
 */
public class JobUtils {
    public static MCJob getJob(JobConfiguration jobConfiguration) {
        MCJob newJob = InjectorUtils.getInjector().getInstance(MCJob.class);
        newJob.initialize(jobConfiguration,null);
        return newJob;
    }
}
