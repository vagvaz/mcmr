package gr.tuc.softnet.core;

import com.google.inject.Singleton;
import org.apache.commons.configuration.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by vagvaz on 5/25/14.
 */
@Singleton
public class LQPConfiguration implements MCConfiguration {
  private  volatile Object mutex = new Object();
  private Logger log = LoggerFactory.getLogger(LQPConfiguration.class);
  private String baseDir;
  private CompositeConfiguration configuration;
  private Map<String, Configuration> configurations;
  private  boolean initialized = false;

  /**
   * Do not instantiate LQPConfiguration.
   */
  public LQPConfiguration() {
    mutex = new Object();
    synchronized (mutex) {
      configuration = new CompositeConfiguration();
      configurations = new HashMap<String, Configuration>();
      initialized = false;
    }
  }

  
  
  /*Default function that initializes the configuration of the current node
  * The fucntion parametrizes specific files such as the jgroups file used by infinsipan
  * This function can take as argument the path of the configuration folder and
  * whether a full or a lite (without the parametrization of files) is needed
  * One can safely use the version with no parameters.*/
  public  void initialize() {
    initialize("conf/", false);
  }

  public  void initialize(String base_dir, boolean lite) {
    synchronized (mutex) {
      if (initialized)
        return;
      initialized = true;
      ConfigurationUtilities.addToClassPath(base_dir);

      if (!base_dir.endsWith("/"))
        base_dir += "/";
      this.setBaseDir(base_dir);
      //Get All important initialValues
      generateDefaultValues();
      resolveDyanmicParameters();
      loadDefaultFiles();
      readPublicIp();
//      if (!lite) {
//        updateConfigurationFiles();
//      }
      System.out.println("Initializing EnsembleCacheUtils");
    }
  }

  private  void readPublicIp() {
    try {
      PropertiesConfiguration publicProperties =
          new PropertiesConfiguration(this.getBaseDir() + "public_ip.properties");
      this.conf().setProperty(StringConstants.PUBLIC_IP,
          publicProperties.getString(StringConstants.PUBLIC_IP, this.conf().getString("node.ip")));
    } catch (ConfigurationException e) {
      System.out.println(e.getMessage());
      log.error(e.getMessage());
    }
  }

  private  void generateDefaultValues() {
    this.conf().setProperty("node.cluster", StringConstants.DEFAULT_CLUSTER_NAME);
    this.conf().setProperty("node.name", StringConstants.DEFAULT_NODE_NAME);
    this.conf().setProperty("processor.InfinispanFile", StringConstants.ISPN_DEFAULT_FILE);
    this.conf().setProperty("processor.JgroupsFile", StringConstants.JGROUPS_DEFAULT_TMPL);
    this.conf().setProperty("processor.debug", "INFO");
    this.conf().setProperty("processor.infinispan.mode", "cluster");
  }

  private  void resolveDyanmicParameters() {
    String hostname = ConfigurationUtilities.resolveHostname();
    this.conf().setProperty("node.hostname", hostname);
    String ip = ConfigurationUtilities.resolveIp();
    this.conf().setProperty("node.ip", ip);
    String broadcast = ConfigurationUtilities.resolveBroadCast(ip);
    this.conf().setProperty("node.broadcast", broadcast);
    log.info("HOST:  " + hostname + " IP " + ip + " BRCD " + broadcast);
  }

  private  void loadDefaultFiles() {
    try {
//      PropertiesConfiguration config = new PropertiesConfiguration(this.getBaseDir() + "processor.properties");
      XMLConfiguration config = new XMLConfiguration(this.getBaseDir()+ "/processor.xml");
      if (config.containsKey("node.interface"))
        this.conf()
            .setProperty("node.ip", ConfigurationUtilities.resolveIp(config.getString("node.interface")));
      this.conf().addConfiguration(config);
      this.getConfigurations().put(this.getBaseDir() + "conf/processor.xml", config);
    } catch (ConfigurationException e) {
      e.printStackTrace();
    }
  }

//  private  void updateConfigurationFiles() {
//    try {
//      XMLConfiguration jgroups = new XMLConfiguration("conf/jgroups.tmpl");
//      String ip = this.conf().getString("node.ip");
//      jgroups.setProperty("TCP[@bind_addr]", "${jgroups.tcp.address:" + ip + "}");
//      //jgroups.setProperty("MPING[@bind_addr]", ip);
//      String broadcast = this.conf().getString("node.broadcast");
//      //                this.conf().getString("node.ip").substring(0, ip.lastIndexOf("."))
//      //                    + ".255";
//      jgroups.setProperty("BPING[@dest]", broadcast);
//      jgroups.save(System.getProperty("user.dir") + "/" + this.getBaseDir() + "jgroups-tcp.xml");
//      //            jgroups.save(baseDir+"jgroups-tcp.xml");
//    } catch (ConfigurationException e) {
//      e.printStackTrace();
//    }
//
//  }

  public  void initialize(boolean lite) {
    initialize("conf/", lite);
  }

  /**
   * Getter for property 'configuration'.
   *
   * @return Value for property 'configuration'.
   */ //Returns the Combined configuration.
  public CompositeConfiguration conf() {
    return configuration;
  }

  /**
   * Setter for property 'configuration'.
   *
   * @param configuration Value to set for property 'configuration'.
   */
  public void setConfiguration(CompositeConfiguration configuration) {
    this.configuration = configuration;
  }

  /**
   * Getter for property 'baseDir'.
   *
   * @return Value for property 'baseDir'.
   */
  public String getBaseDir() {
    return baseDir;
  }

  /**
   * Setter for property 'baseDir'.
   *
   * @param baseDir Value to set for property 'baseDir'.
   */
  public void setBaseDir(String baseDir) {
    this.baseDir = baseDir;
  }

  /**
   * Getter for property 'configurations'.
   *
   * @return Value for property 'configurations'.
   */
  public Map<String, Configuration> getConfigurations() {
    return configurations;
  }

  /**
   * Setter for property 'configurations'.
   *
   * @param configurations Value to set for property 'configurations'.
   */
  public void setConfigurations(Map<String, Configuration> configurations) {
    this.configurations = configurations;
  }

  private void loadPropertiesConfig() {
    File[] files = ConfigurationUtilities.getConfigurationFiles("processor*.properties");
    for (File f : files) {
      try {
        PropertiesConfiguration config = new PropertiesConfiguration(f);
        log.info(f.toString() + " was found and loaded");
        configuration.addConfiguration(config);
      } catch (ConfigurationException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Getter for property 'microClusterName'.
   *
   * @return Value for property 'microClusterName'.
   */
  public String getMicroClusterName() {
    return configuration.getString("node.cluster");
  }

  /**
   * Getter for property 'nodeName'.
   *
   * @return Value for property 'nodeName'.
   */
  public String getNodeName() {
    return configuration.getString("node.name");
  }

  /**
   * Getter for property 'hostname'.
   *
   * @return Value for property 'hostname'.
   */
  public String getHostname() {
    return configuration.getString("node.hostname");
  }

  public void loadFile(String filename) {
    Configuration tmp = null;
    File confFile = new File(filename);
    if (!confFile.exists()) {
      confFile = new File(baseDir + filename);
      if (!confFile.exists()) {
        log.error(
            "File " + filename + "Could not be loaded because it does not exist neither in the working dir nor in "
                + baseDir);
        return;
      }
    }
    if (filename.endsWith(".properties")) {

      try {
        tmp = new PropertiesConfiguration(filename);
      } catch (ConfigurationException e) {
        e.printStackTrace();
      }
    } else if (filename.endsWith(".xml")) {
      try {
        tmp = new XMLConfiguration(filename);
      } catch (ConfigurationException e) {
        e.printStackTrace();
      }

    }
    if (tmp != null) {
      this.getConfigurations().put(filename, tmp);
      this.conf().addConfiguration(tmp);
    }

  }

  public void storeConfiguration(String configName, String target) {
    Configuration tmp = this.getConfigurations().get(configName);
    if (tmp != null) {
      if (configName.endsWith(".properties")) {
        PropertiesConfiguration prop = (PropertiesConfiguration) tmp;
        try {
          prop.save(target);
        } catch (ConfigurationException e) {
          e.printStackTrace();
        }
        log.info("Configuration " + configName + " stored to " + target);
      } else if (configName.endsWith(".xml")) {
        XMLConfiguration xml = (XMLConfiguration) tmp;
        try {
          xml.save(target);
        } catch (ConfigurationException e) {
          e.printStackTrace();
        }
        log.info("Configuration " + configName + " stored to " + target);
      } else {
        log.warn("Unknown configuration type");
      }
    }
  }

  @Override
  public Configuration conf(String configurationName) {
    return this.conf().subset(configurationName);
//    this.conf().getProperties(configurationName);
  }


}
