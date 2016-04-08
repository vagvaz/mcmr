package gr.tuc.softnet.core;

import com.google.inject.AbstractModule;
import gr.tuc.softnet.engine.*;
import gr.tuc.softnet.kvs.KVSManager;
import gr.tuc.softnet.kvs.KVSManagerImpl;
import gr.tuc.softnet.netty.MCDataTransport;
import gr.tuc.softnet.netty.NettyDataTransport;

/**
 * Created by vagvaz on 11/02/16.
 */
public class MCNodeModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(NodeManager.class).to(MCNodeManagerImpl.class);
        bind(MCConfiguration.class).to(LQPConfiguration.class);
        bind(MCDataTransport.class).to(NettyDataTransport.class);
        bind(KVSManager.class).to(KVSManagerImpl.class);
        bind(JobManager.class).to(JobManagerImpl.class);
        bind(TaskManager.class).to(TaskManagerImpl.class);
        bind(MCJob.class).to(MCJobImpl.class);
    }




}
