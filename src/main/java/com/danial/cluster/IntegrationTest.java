package com.danial.cluster;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.tools.ClusterSetup;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IntegrationTest {

    public static void main(String[] args) throws IOException, InterruptedException {
        ZkServer server = null;
        ;

        try
        {
            String baseDir = "/tmp/IntegrationTest/";
            final String dataDir = baseDir + "zk/dataDir";
            final String logDir = baseDir + "zk/logDir";
            FileUtils.deleteDirectory(new File(dataDir));
            FileUtils.deleteDirectory(new File(logDir));

            IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {
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
            setup.addInstanceToCluster(clusterName, "localhost:12001");
            setup.addInstanceToCluster(clusterName, "localhost:12002");
            setup.addInstanceToCluster(clusterName, "localhost:12003");
            setup.addResourceToCluster(clusterName, "repository", 1, "MasterSlave");
            setup.rebalanceResource(clusterName, "repository", 3);

            // Set the configuration
            final String instanceName1 = "localhost_12001";
            addConfiguration(setup, baseDir, clusterName, instanceName1);
            final String instanceName2 = "localhost_12002";
            addConfiguration(setup, baseDir, clusterName, instanceName2);
            final String instanceName3 = "localhost_12003";
            addConfiguration(setup, baseDir, clusterName, instanceName3);
            Thread thread1 = new Thread(new Runnable() {
                @Override
                public void run() {
                    FileStore fileStore = null;

                    try
                    {
                        fileStore = new FileStore(zkAddress, clusterName, instanceName1);
                        fileStore.connect();
                    }
                    catch (Exception e)
                    {
                        System.err.println("Exception: " + e);
                        fileStore.disconnect();
                    }
                }
            });

            // Start node2
            Thread thread2 = new Thread(new Runnable() {
                @Override
                public void run() {
                    FileStore fileStore = new FileStore(zkAddress, clusterName, instanceName2);
                    fileStore.connect();

                }
            });

            // Start node2
            Thread thread3 = new Thread(new Runnable() {
                @Override
                public void run() {
                    FileStore fileStore = new FileStore(zkAddress, clusterName, instanceName3);
                    fileStore.connect();

                }
            });
            System.out.println("Starting nodes ...");
            thread1.start();
            thread2.start();
            thread3.start();

            // Start controller
            final HelixManager manager = HelixControllerMain.startHelixController(zkAddress, clusterName, "controller", HelixControllerMain.STANDALONE);
            Thread.sleep(5000);
            printStatus(manager);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            if (server != null)
            {

            }
        }

        Thread.currentThread().join();
    }

    private static void printStatus(final HelixManager manager)
    {
        System.out.println("CLUSTER STATUS");
        HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
        PropertyKey.Builder keyBuilder = helixDataAccessor.keyBuilder();
        System.out.println("External View \n" + helixDataAccessor.getProperty(keyBuilder.externalView("repository")));
    }

    private static void listFiles(String baseDir)
    {

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
        new File(properties.get("change_log_dir"));
        new File(properties.get("file_store_dir"));
        new File(properties.get("check_point_dir"));

    }
}
