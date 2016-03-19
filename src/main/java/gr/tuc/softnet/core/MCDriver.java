package gr.tuc.softnet.core;

import com.google.inject.Guice;
import com.google.inject.Injector;
import gr.tuc.softnet.engine.MCNode;

/**
 * Created by vagvaz on 02/03/16.
 */
public class MCDriver {
    public static void main(String[] args) {
        Injector injector = Guice.createInjector(new MCNodeModule());
        NodeManager nodemanager = injector.getInstance(NodeManager.class);
        if(args.length > 0 ) {
            nodemanager.initialize(args[0]);
        }else{
            nodemanager.initialize(StringConstants.DEFAULT_CONF_DIR);
        }
        System.out.println(nodemanager.getID());
        System.out.println(nodemanager.getLocalcloudName());

        nodemanager.waitForExit();
    }
}
