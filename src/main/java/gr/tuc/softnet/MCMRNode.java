package gr.tuc.softnet;

import com.google.inject.Guice;
import com.google.inject.Injector;
import gr.tuc.softnet.core.InjectorUtils;
import gr.tuc.softnet.core.MCNodeModule;
import gr.tuc.softnet.core.NodeManager;
import gr.tuc.softnet.core.StringConstants;
import gr.tuc.softnet.engine.TaskManager;

/**
 * Created by vagvaz on 16/11/16.
 */
public class MCMRNode {
  public static void main(String[] args) {
    Injector injector = Guice.createInjector(new MCNodeModule());
    InjectorUtils.setInjector(injector);
    NodeManager nodemanager = injector.getInstance(NodeManager.class);
    if(args.length > 0 ) {
      nodemanager.initialize(args[0]);
    }else{
      nodemanager.initialize(StringConstants.DEFAULT_CONF_DIR);
    }
    TaskManager taskManager = InjectorUtils.getInjector().getInstance(TaskManager.class);
    taskManager.initialize();
    nodemanager.waitForExit();
  }
}
