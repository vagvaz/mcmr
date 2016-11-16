package gr.tuc.softnet.tools;

import com.google.gson.JsonObject;
import com.jcraft.jsch.*;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.configuration.*;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.hadoop.util.hash.Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;

import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by vagvaz on 8/21/14.
 */
public class Bootstrapper {
  private static final String JAR_FILENAME = "~/.mcmr/mcmr.jar";
  static String[] adresses;
  static List<Properties> clusters = null;
  static String[] configurationFiles;
  static String filename;
  static Logger logger;
  static Map<String, HierarchicalConfiguration> componentsXml = new HashedMap();
  static Map<String, Configuration> componentsConf = new HashedMap();
  static Map<String, Integer> componentsInstances = new HashedMap();

  static Map<String, String> screensIPmap = new HashedMap();
  static Map<String, Configuration> IPsshmap = new HashedMap();
  static Map<String, Configuration> sshmap = new HashedMap();


  static XMLConfiguration xmlConfiguration = null;
  static CompositeConfiguration conf = null;
  //  static Configuration commonConfiguration = null;
  static String baseDir = "";
  static int sleepTime = 5;

  static int pagecounter = 0;
  static HashMap<String, Configuration> allmcAddrs = new HashMap<>();
  static String start_date_time;
  static int module_deploy_num = 0;
  static boolean deploy = false;
  private static boolean norun;
  private static XMLConfiguration basicConfig = new XMLConfiguration();

  public static void main(String[] args) {

    start_date_time = new SimpleDateFormat("HHmmss").format(new Date());
    readConfiguration(args);
    initSSH();
    getClusterAdresses();
    init_components_configuration();
    sleepTime = conf.getInt("startUpDelay", 2);
    deploy();
  }

  public static void readConfiguration(String[] args) {
    org.apache.log4j.BasicConfigurator.configure();

    logger = LoggerFactory.getLogger(Bootstrapper.class.getCanonicalName());

    componentsXml = new HashMap<>();
    componentsConf = new HashMap<>();
    componentsInstances = new HashMap<>();
    screensIPmap = new HashMap<>();


    conf = new CompositeConfiguration();

    if (checkArguments(args)) {
      if (args[0].startsWith("deploy")) {
        deploy = true;
      } else {
        deploy = false;
      }
      filename = args[1];
    } else {
      logger.error("Incorrect arguments  ");
      System.err.println("Incorrect arguments  ");
      System.exit(-1);
    }

    try {
      xmlConfiguration = new XMLConfiguration(filename);
    } catch (ConfigurationException e) {
      e.printStackTrace();
      System.err.println("Unable to load configuration, Please check the file: " + filename);
      System.exit(-1);
    }
    conf.addConfiguration(xmlConfiguration);

    baseDir =
      getStringValue(conf, "baseDir", null, true); //FIX IT using pwd and configuration file path!?

  }

  public static void getClusterAdresses() {
    adresses = conf.getStringArray("adresses");

    Properties cluster;
    HierarchicalConfiguration nodeData;
    HierarchicalConfiguration network = new XMLConfiguration();


    List<HierarchicalConfiguration> cmplXadresses =
      ((XMLConfiguration) xmlConfiguration).configurationsAt("adresses.MC");
    if (cmplXadresses == null) { //
      logger.error("Clusters Addresses not found");
      return;
    }
    clusters = new ArrayList<>();
    IPsshmap = new HashMap<>();
    List<HierarchicalConfiguration> mcs = new ArrayList<>();
    for (HierarchicalConfiguration c : cmplXadresses) {
      HierarchicalConfiguration currentMc = new HierarchicalConfiguration();
      List<HierarchicalConfiguration.Node> mcNodes = new ArrayList<>();

      List<Configuration> nodesList = new ArrayList<>();
      List<String> clusterPublicIPs = new ArrayList<>();
      ;
      List<String> clusterPrivateIPs = new ArrayList<>();
      ;
      List<Integer> ports = new ArrayList<>();
      ;
      List<String> devInterfaces = new ArrayList<>();
      ;
      ConfigurationNode node = c.getRoot();
      System.out.println("Found Cluster : " + c.getString("[@name]"));
      System.out.println("Cluster Size : " + node.getChildren().size());
      cluster = new Properties();
      cluster.setProperty("name", c.getString("[@name]"));
      currentMc.addProperty("name", c.getString("[@name]"));
      cluster.setProperty("credentials", c.getString("[@credentials]"));
      List<HierarchicalConfiguration> nodes = c.configurationsAt("node");
      nodeData = new HierarchicalConfiguration();
      clusterPublicIPs = new ArrayList<>();
      clusterPrivateIPs = new ArrayList<>();
      for (HierarchicalConfiguration n : nodes) {
        nodeData = new HierarchicalConfiguration();
        String prIP = n.getString("[@privateIp]");
        String puIP = n.getString("");
        String name = n.getString("[@name]");
        Integer port = n.getInt("[@port]", 10000);
        String devInterface = n.getString("[@interface]", "wlan0");
        if (puIP != null) {
          clusterPublicIPs.add(puIP);
          nodeData.setProperty("publicIp", puIP);
          IPsshmap.put(puIP, sshmap.get(c.getString("[@credentials]")));
        }
        if (prIP != null) {
          nodeData.setProperty("privateIp", prIP);
          clusterPrivateIPs.add(prIP);
          IPsshmap.put(prIP, sshmap.get(c.getString("[@credentials]")));
        }
        currentMc.addProperty("node", puIP + ":" + Integer.toString(port));
        devInterfaces.add(devInterface);
        ports.add(port);
        clusterPrivateIPs.add(prIP);
        nodeData.setProperty("name", name);
        nodeData.setProperty("port", port);
        nodeData.setProperty("interface", devInterface);
        System.out.println("puIP: " + puIP + " name: " + name + " prIP: " + prIP);
        nodesList.add(nodeData);
      }
      cluster.put("nodes", nodesList);
      cluster.put("allPublicIps", clusterPublicIPs);
      cluster.put("allPrivateIps", clusterPrivateIPs);
      cluster.put("interfaces", devInterfaces);
      cluster.put("ports", ports);
      clusters.add(cluster);
      currentMc.getRootNode().setName("mc");
      network.getRootNode().addChild(currentMc.getRootNode());
    }
    network.getRootNode().setName("network");
    xmlConfiguration.getRootNode().addChild(network.getRootNode());
    XMLConfiguration tmp = new XMLConfiguration(xmlConfiguration) {
      @Override protected Transformer createTransformer() throws TransformerException {
        Transformer transformer = super.createTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
        return transformer;
      }
    };
    try {
      tmp.save("/tmp/checkthisout.xml");
      //      tmp.save();


    } catch (ConfigurationException e) {
      e.printStackTrace();
    }
  }


  public static void init_components_configuration() {
    HierarchicalConfiguration config = new HierarchicalConfiguration();
    config.setRoot(new HierarchicalConfiguration.Node("mcmapreduce"));
    List<HierarchicalConfiguration> nodes = new ArrayList<>();
    nodes.add(new HierarchicalConfiguration(xmlConfiguration.configurationAt("kvs")));
    nodes.add(new HierarchicalConfiguration(xmlConfiguration.configurationAt("engine")));
    nodes.add(new HierarchicalConfiguration(xmlConfiguration.configurationAt("network")));
    nodes.get(0).getRootNode().setName("kvs");
    nodes.get(1).getRootNode().setName("engine");
    nodes.get(2).getRootNode().setName("network");
    basicConfig.getRootNode().addChild(nodes.get(0).getRootNode());
    basicConfig.getRootNode().addChild(nodes.get(1).getRootNode());
    basicConfig.getRootNode().addChild(nodes.get(2).getRootNode());



  }

  public static void initSSH() {
    List<HierarchicalConfiguration> components =
      ((XMLConfiguration) xmlConfiguration).configurationsAt("ssh.credentials");
    if (components == null) { //Maybe components is jsut empty...
      logger.error("No ssh credentials found exiting");
      System.exit(-1);
    }

    for (HierarchicalConfiguration c : components) {
      ConfigurationNode node = c.getRootNode();
      HierarchicalConfiguration ssh = new HierarchicalConfiguration();
      String id = c.getString("id");
      if (id == null) {
        System.out.println("Bad formatted ssh credentials");
        continue;
      }
      ssh.addProperty("id", id);
      Iterator<String> ks = c.getKeys();
      while (ks.hasNext()) {
        String key = ks.next();
        ssh.addProperty(key, c.getString(key));
      }
      sshmap.put(id, ssh);
    }
  }


  public static void deploy() {
    //multicloud deployment
    System.out.println("Multi-cloud deployment");
    JSch jsch = new JSch();
    for (Properties c : clusters) {
      List<HierarchicalConfiguration> nodes = (List<HierarchicalConfiguration>) c.get("nodes");

      //      List<Object> publicIps = (List<Object>) c.getProperty("allPublicIps");
      //      List<Object> privateIps = (List<Object>) c.getProperty("allPrivateIps");
      //      List<Object> ports =  (List<Object>) c.getProperty("ports");
      //      List<Object> interfaces =  (List<Object>) c.getProperty("interfaces");
      String credentialsKey = c.getProperty("credentials");
      for (int i = 0; i < nodes.size(); i++) {
        String nodeName = (String) nodes.get(i).getString("name");
        String publicip = nodes.get(i).getString("publicIp");
        String privateIp = nodes.get(i).getString("privateIp");
        String devInterface = nodes.get(i).getString("interface");
        Integer port = nodes.get(i).getInt("port");
        //        String publicip = (String) publicIps.get(i);
        //        String privateIp = (String) privateIps.get(i);
        //        Integer port = (Integer) ports.get(i);
        //        String devInterface = (String) interfaces.get(i);
        Configuration credentials = sshmap.get(credentialsKey);
        try {
          Session session = createSession(jsch, publicip);
          Channel channel = session.openChannel("exec");
          XMLConfiguration config =
            buildConfiguration(c.getProperty("name"), nodeName, publicip, privateIp, devInterface,
              port);
          config.save("/tmp/__tmp__file.xml");
          sendConfigurationTo("/tmp/__tmp__file.xml", publicip, nodeName);
          deployToMachine(publicip, "mcmr_" + nodeName + ".xml");
        } catch (JSchException e) {
          e.printStackTrace();
        } catch (ConfigurationException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void deployToMachine(String publicip, String configName) {
    String command = "java -jar " + JAR_FILENAME + " " + configName;
    remoteExecute(publicip, command);
  }

  private static XMLConfiguration buildConfiguration(String name, String nodeName, String publicip,
    String privateIp, String devInterface, Integer port) {
    XMLConfiguration result = new XMLConfiguration(basicConfig);
    HierarchicalConfiguration nodeConfig = new HierarchicalConfiguration();
    nodeConfig.setProperty("id", nodeName);
    nodeConfig.setProperty("cluster", name);
    nodeConfig.setProperty("public_ip", publicip);
    nodeConfig.setProperty("interface", devInterface);
    nodeConfig.setProperty("port", port);
    nodeConfig.getRootNode().setName("node");
    result.getRootNode().addChild(nodeConfig.getRootNode());
    return result;
  }

  public static void kill() {
    for (Properties c : clusters) {
      List<Object> nodes = (List<Object>) c.get("nodes");
      List<Object> publicIps = (List<Object>) c.get("allPublicIps");
      List<Object> privateIps = (List<Object>) c.get("allPrivateIps");
      List<Object> ports = (List<Object>) c.get("ports");
      List<Object> interfaces = (List<Object>) c.get("interfaces");
      String credentialsKey = (String) c.get("credentials");
      for (int i = 0; i < nodes.size(); i++) {
        String nodeName = (String) nodes.get(i);
        String publicip = (String) publicIps.get(i);
        String privateIp = (String) privateIps.get(i);
        Integer port = (Integer) ports.get(i);
        String devInterface = (String) interfaces.get(i);
        Configuration credentials = sshmap.get(credentialsKey);
        killMachine(nodeName, publicip);
      }
    }
  }

  private static void killMachine(String nodeName, String publicip) {
    String command = "pgrep java | xargs kill -9";
    logger.info("Killing machines for " + nodeName + " @" + publicip);
    remoteExecute(publicip, command);
  }

  public static String get_date_unique_id() {
    return start_date_time + Integer.toString(module_deploy_num++);
  }


  private static String getStringValue(Configuration conf, String key, String default_value,
    boolean on_error_exit) {
    String ret = conf.getString(key);
    if (ret == null)
      if (on_error_exit) {
        logger.error("Required value " + key + " undefined, bootStrapper exits");
        System.exit(13);
      } else {
        logger.warn("Required value " + key + " undefined, bootStrapper, continues with value "
          + default_value);
        ret = default_value;
      }
    return ret;
  }


  //  private static void deployComponent(String component, JsonObject modJson, String ip) {
  //    //JsonObject modJson = componentsJson.get(component); //generateConfiguration(component);
  //    if (modJson == null)
  //      return;
  //
  //    sendConfigurationTo(modJson, ip);
  //
  //    String group = getStringValue(conf, "processor.groupId", null, true);
  //    String version = getStringValue(conf, "processor.version", null, true);
  //    ;
  //    //    String command = "vertx runMod " + group +"~"+ component + "-mod~" + version + " -conf /tmp/"+config.getString("id")+".json";
  //    String remotedir = getStringValue(conf, "remoteDir", null, true);
  //    String vertxComponent;
  //    if (modJson.containsField("modName"))
  //      vertxComponent = group + "~" + modJson.getString("modName") + "~" + version;
  //    else
  //      vertxComponent = group + "~" + component + "-comp-mod~" + version;
  //
  //    String command;
  //    String vertx_executable;
  //    if (vertxComponent.contains("web"))
  //      vertx_executable = "vertx";
  //    else if (vertxComponent.contains("log"))//run vertx with lower memory usage (vertxb)
  //      vertx_executable = "vertx";
  //    else if (vertxComponent.contains("iman"))
  //      vertx_executable = "vertx";
  //    else if (vertxComponent.contains("dep"))
  //      vertx_executable = "vertx";
  //    else if (vertxComponent.contains("nqe"))
  //      vertx_executable = "vertx";
  //    else if (vertxComponent.contains("plan"))
  //      vertx_executable = "vertx";
  //    else
  //      vertx_executable = "vertx";
  //
  //    command = vertx_executable + " runMod " + vertxComponent + " -conf " + remotedir + "R" + modJson
  //      .getString("id") + ".json";
  //    if (conf.containsKey("processor.vertxArg"))
  //      command += " -" + conf.getString("processor.vertxArg");
  //
  //    runRemotely(modJson.getString("id"), ip, command);
  //    for (int t = 0; t < sleepTime * 2; t++) {
  //      System.out.printf(
  //        "\rPlease wait " + sleepTime + "s Deploying: " + component + " elapsed:" + Integer
  //          .toString(1 + t * 500 / 1000));
  //      try {
  //        Thread.sleep(500);
  //      } catch (InterruptedException e1) {
  //        e1.printStackTrace();
  //      }
  //    }
  //    System.out.println("");
  //  }

  public static void runRemotely(String id, String ip, String command) {
    runRemotely(id, ip, command, false);
  }

  public static void runRemotely(String id, String ip, String command, boolean existingWindow) {
    String command1;
    if (existingWindow) {
      String session_name;
      if (screensIPmap.containsKey(ip)) {
        session_name = screensIPmap.get(ip);
        // run top within that bash session
        String command0 = "cd ~/.mcmr && screen -S " + session_name;
        // run top within that bash session
        command1 =
          command0 + " screen -S  " + session_name + " -p " + (pagecounter++) + " -X stuff $\'"
            + " bash -l &&" + command + "\r\'";//ping 147.27.18.1";
      } else {
        session_name = "shell_" + ip;
        screensIPmap.put(ip, session_name);
        String command0 = "cd ~/.mcmr && screen -AmdS " + session_name + " bash -l";
        // run top within that bash session
        command1 = command0 + " && " + "screen -S  " + session_name
          + " -p \" + (pagecounter++) +\" -X stuff $\'" + command + "\r\'";//ping 147.27.18.1";
      }

    } else {
      String command0 = "cd ~/.mcmr &&  screen -AmdS shell_" + id + " bash -l";
      // run top within that bash session
      command1 = command0 + " && " + "screen -S shell_" + id + " -p 0 -X stuff $\'" + command
        + "\r\'";//ping 147.27.18.1";
    }

    //System.out.print("Cmd: " + command1);
    logger.info("Execution command: " + command1);
    //command1 =command;
    remoteExecute(ip, command1);

  }

  private static String remoteExecute(String ip, String command_) {
    Configuration sshconf = IPsshmap.get(ip);
    String ret = "";
    if (norun && !command_.contains("salt")) {
      System.out.print("Just local test, no remote execution");
      return ret;
    }
    try {
      JSch jsch = new JSch();

      Session session = createSession(jsch, ip);
      Channel channel = session.openChannel("exec");

      if (command_.contains("sudo") && sshconf.containsKey("password"))
        command_ = command_.replace("sudo", "sudo -S -p '' "); //check if works always

      ((ChannelExec) channel).setCommand(command_);
      OutputStream out = channel.getOutputStream();
      channel.setInputStream(null);
      ((ChannelExec) channel).setErrStream(System.err);
      //((ChannelExec) channel).setPty(true);

      InputStream in = channel.getInputStream();
      channel.connect();
      if (command_.contains("sudo") && sshconf.containsKey("password")) {
        out.write((sshconf.getString("password") + "\n").getBytes());
        out.flush();
      }
      byte[] tmp = new byte[1024];
      while (true) {
        while (in.available() > 0) {
          int i = in.read(tmp, 0, 1024);
          if (i < 0)
            break;
          // System.out.print(new String(tmp, 0, i));
          ret += new String(tmp, 0, i);
        }

        if (channel.isClosed()) {
          logger.info("exit-status: " + channel.getExitStatus());
          break;
        }
        try {
          Thread.sleep(200);
        } catch (Exception ee) {
        }
      }
      if (!ret.isEmpty())
        System.out.println(ret);

      channel.disconnect();
      session.disconnect();

      if (channel.getExitStatus() == 0)
        logger.info("Remote execution DONE");
      else
        logger.error("Unsuccessful execution");

    } catch (Exception e) {
      logger.error("Remote execution error: " + e.getMessage());
    }
    return ret;
  }


  private static boolean sendConfigurationTo(String configFileName, String ip, String node) {
    String remoteDir = getStringValue(conf, "remoteDir", null, true);
    String tmpFile = "processor.xml";
    String remotedir = getStringValue(conf, "remoteDir", null, true) + "/conf/" + node;
    String mkdirCommand = "mkdir -p " + remotedir + "/conf/"+node;
    remoteExecute(ip, mkdirCommand);
    if (norun) {
      System.out.println("No configuration upload, just testing, file created  " + tmpFile);
      return true;
    }
    JSch jsch = new JSch();


    Session session;
    try {
      session = createSession(jsch, ip);
      ChannelSftp channel = (ChannelSftp) session.openChannel("sftp");
      channel.connect();
      File localFile = new File(configFileName);
      //If you want you can change the directory using the following line.
      channel.cd("/");
      channel.put(new FileInputStream(localFile), remotedir+"/"+tmpFile);
      channel.disconnect();
      session.disconnect();
      logger.info("File successful uploaded: " + localFile);
      return true;

    } catch (JSchException e) {
      e.printStackTrace();
      logger.error("Ssh error: " + e.getMessage());
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      logger.error("File not found : " + e.getMessage());
    } catch (SftpException e) {
      logger.error("Sftp error: " + e.getMessage());
      e.printStackTrace();
    }
    return false;

  }

  private static Session createSession(JSch jsch, String ip) throws JSchException {
    Configuration sshconf = IPsshmap.get(ip);
    if (sshconf == null) {
      System.err.println("No ssh configuration for ip: " + ip);
      System.exit(-1);
    }
    String username =
      sshconf.getString("username");//getStringValue(conf, "ssh.username", null, true);
    Session session = jsch.getSession(username, ip, 22);

    if (sshconf.containsKey("rsa")/*xmlConfiguration.containsKey("ssh.rsa")*/) {
      String privateKey = sshconf.getString("rsa"); //conf.getString("ssh.rsa");
      logger.info("ssh identity added: " + privateKey);
      jsch.addIdentity(privateKey);
      session = jsch.getSession(username, ip, 22);
    } else if (sshconf.containsKey("password")/*xmlConfiguration.containsKey("ssh.password")*/)
      session.setPassword(sshconf.getString("password")/*conf.getString("ssh.password")*/);
    else {
      logger.error("No ssh credentials, no password either key ");
      System.out.println("No ssh credentials, no password either key ");
      System.exit(-1);
    }
    session.setConfig("StrictHostKeyChecking", "no");
    jsch.setKnownHosts("~/.ssh/known_hosts");
    session.connect();
    logger.info("Securely connected to " + ip);
    //System.out.println("Connected");
    return session;
  }


  private static boolean checkArguments(String[] args) {
    boolean result = false;
    if (args.length >= 2) {
      if (args[0].startsWith("deploy") || args[0].startsWith("kill")) {
        if (args[1].toLowerCase().endsWith(".xml"))
          result = true;
        else {
          logger.error("Argument 1 must be the boot-configuraion.xml [Xml] file!");
          System.exit(-1);
        }
      } else {
        logger.error("Argument 0 must be the deploy or kill");
        System.exit(-1);
      }
      if (args.length >= 3)
        if (args[1].contains("norun"))
          norun = true;
    }
    return result;
  }

}
