package gr.tuc.softnet.engine;

import gr.tuc.softnet.core.MCConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;

/**
 * Created by vagvaz on 10/02/16.
 */
public class JobStatus implements Statusable,IDable {
    String id;
    MCJob job;
    HierarchicalConfiguration configuration;
    public JobStatus(String id){
        this.id = id;
        configuration = new XMLConfiguration();
    }

    public JobStatus(MCJob job){
        this.id = job.getID();
        this.job = job;
        configuration = new HierarchicalConfiguration();
        configuration.append(job.getConfiguration());
    }
    public String getID() {
        return id;
    }

    public MCConfiguration getConfiguration() {
        return configuration;
    }

    public Configuration getStatus() {
        return configuration;
    }

    public void updateStatus(String name, String value){
        configuration.addProperty(name,value);
    }

    public void updateStatus(Configuration configuration){
        this.configuration.append(configuration);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public MCJob getJob() {
        return job;
    }

    public void setJob(MCJob job) {
        this.job = job;
    }

    public void setConfiguration(HierarchicalConfiguration configuration) {
        this.configuration = configuration;
    }
}
