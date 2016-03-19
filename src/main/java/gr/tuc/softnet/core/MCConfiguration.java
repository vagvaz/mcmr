package gr.tuc.softnet.core;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;

import java.util.Map;

/**
 * Created by vagvaz on 11/02/16.
 */
public interface MCConfiguration {
    /*Default function that initializes the configuration of the current node
  * The fucntion parametrizes specific files such as the jgroups file used by infinsipan
  * This function can take as argument the path of the configuration folder and
  * whether a full or a lite (without the parametrization of files) is needed
  * One can safely use the version with no parameters.*/
    void initialize();

    void initialize(String base_dir, boolean lite);

    void initialize(boolean lite); //Returns the Combined configuration.

    CompositeConfiguration conf();

    void setConfiguration(CompositeConfiguration configuration);

    String getBaseDir();

    void setBaseDir(String baseDir);

    Map<String, Configuration> getConfigurations();

    void setConfigurations(Map<String, Configuration> configurations);

    String getMicroClusterName();

    String getNodeName();

    String getHostname();

    void loadFile(String filename);

    void storeConfiguration(String configName, String target);

    Configuration conf(String configurationName);

    String getURI();
}
