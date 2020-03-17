package com.danial.cluster;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.examples.MasterSlaveStateModelFactory;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.tools.ClusterSetup;

import org.apache.helix.model.HelixConfigScope;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.util.*;


public class HelixDemo {


    private static final String STATE_MODEL_NAME = "MyStateModel";
    private static String ZK_ADDRESS = "localhost:2199";
    private static String CLUSTER_NAME = "helix-demo";
    private static String RESOURCE_NAME = "MyDB";
    private static int NUM_NODES = 2;
//    private static int NUM_PARTITIONS = 6;
    private static int NUM_PARTITIONS = 32;
    private static int NUM_REPLICAS = 2;

    private static List<InstanceConfig> INSTANCE_CONFIG_LIST;
    private static List<MyProcess> PROCESS_LIST;
    private static ZKHelixAdmin admin;

    static
    {
        INSTANCE_CONFIG_LIST = new ArrayList<InstanceConfig>();
        PROCESS_LIST = new ArrayList<HelixDemo.MyProcess>();
        for (int i = 0; i < NUM_NODES; i++)
        {
            int port = 12000 + i;
            InstanceConfig instanceConfig = new InstanceConfig("localhost_" + port);
            instanceConfig.setHostName("localhost");
            instanceConfig.setPort("" + port);
            instanceConfig.setInstanceEnabled(true);
            INSTANCE_CONFIG_LIST.add(instanceConfig);
        }
    }

    public static void main(String[] args) throws Exception {


        /*
         * HelixDemo1
        startZookeeper();
        setup();
        startNodes();
        startController();
        Thread.sleep(5000);
        printState("After starting 2 nodes");
        addNode();
        Thread.sleep(5000);
        printState("After adding a third node");
        addNode();
        Thread.sleep(5000);
        printState("After adding a 4th node");
        stopNode();
        Thread.sleep(5000);
        printState("After the 4th node stops/crashes");
        echo(Thread.currentThread().getId());
        Thread.currentThread().join();
        System.exit(0);

         */

        /*
         * rsync-replicated-file-system
         */

        ZkServer server = null;

        try
        {
            String baseDir = "/tmp/IntegrationTest"/;
            final String dataDir = baseDir + "zk/dataDir";
            final String logDir = baseDir + "/tmp/logDir";
            FileUtils.deleteDirectory(new File(dataDir));
            FileUtils.deleteDirectory(new File(logDir));

            IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace(){
                @Override
                public void createDefaultNameSpace(ZkClient zkClient) {

                }
            };
            int zkPort = 2199;
            final String zkAddress = "localhost:" + zkPort;

            server = new ZkServer(dataDir, logDir, defaultNameSpace, zkPort);
            server.start();

            ClusterSetup setup = new ClusterSetup(zkAddress);
            final String clusterName = "file-store-test";
            setup.deleteCluster(clusterName);
            setup.addCluster(clusterName, true);
            setup.addInstanceToCluster(clusterName, "localhost_12001");
            setup.addInstanceToCluster(clusterName, "localhost_12002");
            setup.addInstanceToCluster(clusterName, "localhost_12003");
            setup.addResourceToCluster(clusterName, "repository", 1, "MasterSlave");
            setup.rebalanceResource(clusterName, "repository", 3);
            // set the configuration
            final String instanceName1 = "localhost_12001";
            addConfiguration(setup, baseDir, clusterName, instanceName1);
            final String instanceName2 = "localhost_12002";
            addConfiguration(setup, baseDir, clusterName, instanceName1);
            final String instanceName3 = "localhost_12003";
            addConfiguration(setup, baseDir, clusterName, instanceName1);
            Thread thread1 = new Thread((Runnable)()->{
                FileStore fileStore = null;

                try
                {
                    fileStore = new FileStore(zkAddress, clusterName, instanceName1);
                    fileStore.connect();

                } catch(Exception e)
                {
                    System.err.println("Exception: " + e);
                    fileStore.disconnect();
                }
            });

        }

    }

    private static void addConfiguration(ClusterSetup setup, String baseDir, String clusterName, String instanceName) throws IOException {
        Map<String, String> properties = new HashMap<String, String>();
        HelixConfigScopeBuilder builder = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT);
        HelixConfigScope instanceScope = builder.forCluster(clusterName).forParticipant(instanceName).build();
        properties.put("change_log_dir", baseDir + instanceName + "/translog");
        properties.put("file_store_dir", baseDir + instanceName + "/filestore");
        properties.put("check_point_dir", baseDir + instanceName + "/checkpoint");
        setup.getClusterManagementTool().setConfig(instanceScope, properties);
        FileUtils.deleteDirectory(new File(properties.get("change_log_dir")));
        FileUtils.deleteDirectory(new File(properties.get("file_store_dir")));
        FileUtils.deleteDirectory(new File(properties.get("check_point_dir")));
        new File(properties.get("change_log_dir")).mkdirs();
        new File(properties.get("file_store_dir")).mkdirs();
        new File(properties.get("check_point_dir")).mkdirs();
    }

    public static void setup()
    {
        // Create setup tool instance
        // Note: ZK_ADDRESS is the host:port of Zookeeper
        admin = new ZKHelixAdmin(ZK_ADDRESS);

        // Create cluster namespace in zookeeper
        echo("Creating cluster: " + CLUSTER_NAME);
        admin.addCluster(CLUSTER_NAME, true);

        // Add nodes to the cluster
        echo("Adding " + NUM_NODES + " participants to the cluster");
        for (int i = 0; i < NUM_NODES; i++)
        {
            admin.addInstance(CLUSTER_NAME, INSTANCE_CONFIG_LIST.get(i));
            echo("\t Added participant: " + INSTANCE_CONFIG_LIST.get(i).getInstanceName());
        }

        // Add a state model
        StateModelDefinition myStateModel = defineStateModel();
        System.out.println("Configuring StateModel: " + "MyStateModel with 1 Master and 1 Slave");
        admin.addStateModelDef(CLUSTER_NAME, STATE_MODEL_NAME, myStateModel);

        // Add a resource with 6 partitions and 2 replicas
        System.out.println("Adding a resource MyDB: " + "with 6 partitions and 2 replicas");
        String MODE = "AUTO";
        admin.addResource(CLUSTER_NAME, RESOURCE_NAME, NUM_PARTITIONS, STATE_MODEL_NAME, MODE);
        // This will set up the ideal state, it calculates the preference list for
        // each partition similar to consistent hashing
        admin.rebalance(CLUSTER_NAME, RESOURCE_NAME, NUM_REPLICAS);

    }

    public static void startNodes() throws Exception {
        echo("Starting participants");
        for (int i = 0; i < NUM_NODES; i++)
        {
            MyProcess process = new MyProcess(INSTANCE_CONFIG_LIST.get(i).getId());
            PROCESS_LIST.add(process);
            process.start();
            echo("\t Started participant: " + INSTANCE_CONFIG_LIST.get(i).getId());
        }
    }

    public static void startController()
    {
        // start controller
        System.out.println("Starting helix controller");
        HelixControllerMain.startHelixController(ZK_ADDRESS, CLUSTER_NAME, "localhost_9100", HelixControllerMain.STANDALONE);
    }

    private static void addNode() throws Exception {
        NUM_NODES = NUM_NODES + 1;
        int port = 12000 + NUM_NODES - 1;
        InstanceConfig instanceConfig = new InstanceConfig("localhost_" + port);
        instanceConfig.setHostName("localhost");
        instanceConfig.setPort("" + port);
        instanceConfig.setInstanceEnabled(true);
        echo("Adding new node: " + instanceConfig.getInstanceName() + ". Partitions will move from old nodes to the new node.");

        ZNRecord record = instanceConfig.getRecord();
//        record.setSimpleField("change_log_dir", "data/localhost_12001/translog");
//        record.setSimpleField("file_store_dir", "data/localhost_12001/filestore");
//        record.setSimpleField("check_point_dir", "data/localhost_12001/checkpoint");


        record.setSimpleField("change_log_dir", "/tmp/localhost_" + port + "/data/translog");
        record.setSimpleField("file_store_dir", "/tmp/localhost_" + port + "/data/filestore");
        record.setSimpleField("check_point_dir", "/tmp/localhost_" + port + "/data/checkpoint");

        admin.addInstance(CLUSTER_NAME, instanceConfig);
        INSTANCE_CONFIG_LIST.add(instanceConfig);
        MyProcess process = new MyProcess(instanceConfig.getInstanceName());
        PROCESS_LIST.add(process);
///        admin.rebalance(CLUSTER_NAME, RESOURCE_NAME, 3);
        admin.rebalance(CLUSTER_NAME, RESOURCE_NAME, 3);
        process.start();


    }

    private static void stopNode()
    {
        int nodeId = NUM_NODES - 1;
        echo("Stopping " + INSTANCE_CONFIG_LIST.get(nodeId).getInstanceName() + ". Mastership will be transferred to the remaining nodes");
        PROCESS_LIST.get(nodeId).stop();
    }

    private static void printState(String msg)
    {
        System.out.println("CLUSTER STATE: " + msg);
        ExternalView resourceExternalView = admin.getResourceExternalView(CLUSTER_NAME, RESOURCE_NAME);
        TreeSet<String> sortedSet = new TreeSet<String>(resourceExternalView.getPartitionSet());
        StringBuilder sb = new StringBuilder("\t\t");
        for (int i = 0; i < NUM_NODES; i++)
        {
            sb.append(INSTANCE_CONFIG_LIST.get(i).getInstanceName()).append("\t");
        }
        System.out.println(sb);
        for (String partitionName : sortedSet)
        {
            sb.delete(0, sb.length() - 1);
            sb.append(partitionName).append("\t");
            for (int i = 0; i < NUM_NODES; i++)
            {
                Map<String, String> stateMap = resourceExternalView.getStateMap(partitionName);
                if (stateMap != null && stateMap.containsKey(INSTANCE_CONFIG_LIST.get(i).getInstanceName()))
                {
                    sb.append(stateMap.get(INSTANCE_CONFIG_LIST.get(i).getInstanceName()).charAt(0)).append("\t\t");
                }
                else
                {
                    sb.append("-").append("\t\t");
                }
            }
            System.out.println(sb);
        }
        System.out.println("##############################################################################");
    }


    public static void echo(Object obj)
    {
        System.out.println(obj);
    }


    private static StateModelDefinition defineStateModel()
    {
        StateModelDefinition.Builder builder = new StateModelDefinition.Builder(STATE_MODEL_NAME);
        // Add states and their rank to indicate priority. A lower rank corresponds to a higher priority
        builder.addState("MASTER", 1);
        builder.addState("SLAVE", 2);
        builder.addState("OFFLINE");
        builder.addState("DROPPED");

        // Set the initial state when the node starts
        builder.initialState("OFFLINE");

        // Add transitions between the states
        builder.addTransition("OFFLINE", "SLAVE");
        builder.addTransition("SLAVE", "OFFLINE");
        builder.addTransition("SLAVE", "MASTER");
        builder.addTransition("MASTER", "SLAVE");
        builder.addTransition("OFFLINE", "DROPPED");

        // set constraints on states
        // static constraint: upper bound of 1 MASTER
        builder.upperBound("MASTER", 1);

        // dynamic constraint: R means it should be derived based on the replication factor for the cluster
        // this allows a different replication factor for each resource without having
        // to define a new state model

        builder.dynamicUpperBound("SLAVE", "R");
        StateModelDefinition stateModelDefinition = builder.build();

        return stateModelDefinition;
    }

    static final class MyProcess
    {
        private final String instanceName;
        private HelixManager manager;

        public MyProcess(String instanceName)
        {
            this.instanceName = instanceName;
        }

        public void start() throws Exception {
            manager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, instanceName, InstanceType.PARTICIPANT, ZK_ADDRESS);
            MasterSlaveStateModelFactory stateModelFactory = new MasterSlaveStateModelFactory(instanceName);
            StateMachineEngine stateMachineEngine = manager.getStateMachineEngine();
            stateMachineEngine.registerStateModelFactory(STATE_MODEL_NAME, stateModelFactory);
            manager.connect();
        }

        public void stop()
        {
            manager.disconnect();
        }
    }

    public static void startZookeeper()
    {
        echo("Start zookeeper at " + ZK_ADDRESS);
        String zkDir = ZK_ADDRESS.replace(':', '_');
        final String logDir = "/tmp/" + zkDir + "/logs";
        final String dataDir = "/tmp/" + zkDir + "/dataDir";
        new File(logDir).mkdirs();
        new File(dataDir).mkdirs();
        IDefaultNameSpace defaultNamespace = new IDefaultNameSpace() {
            @Override
            public void createDefaultNameSpace(ZkClient zkClient) {

            }
        };

        int port = Integer.parseInt(ZK_ADDRESS.substring(ZK_ADDRESS.lastIndexOf(':') + 1));
        ZkServer server = new ZkServer(dataDir, logDir, defaultNamespace,port);
        server.start();

    }
}
