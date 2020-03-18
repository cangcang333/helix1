package com.danial.cluster;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.participant.StateMachineEngine;

public class FileStore {
    private final String _zkAddr;
    private final String _clusterName;
    private final String _serverId;
    private HelixManager _manager = null;

    public FileStore(String zkAddr, String clusterName, String serverId)
    {
        _zkAddr = zkAddr;
        _clusterName = clusterName;
        _serverId = serverId;
    }

    public void connect()
    {
        try
        {
            _manager = HelixManagerFactory.getZKHelixManager(_clusterName, _serverId, InstanceType.PARTICIPANT, _zkAddr);
            StateMachineEngine stateMach = _manager.getStateMachineEngine();
            FileStoreStateModelFactory modelFactory = new FileStoreStateModelFactory(_manager);
            stateMach.registerStateModelFactory(SetupCluster.DEFAULT_STATE_MODEL, modelFactory);
            _manager.connect();


//            _manager.addExternalViewChangeListener(replicator);    // What does this do
            Thread.currentThread().join();
        }
        catch (InterruptedException e)
        {
            System.err.println(" [-] " + _serverId + " is interrupted ... ");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            _manager.disconnect();
        }
    }

    public void disconnect()
    {
        if (_manager != null)
        {
            _manager.disconnect();
        }
    }

    public static void main(String[] args)
    {
        if (args.length < 2)
        {
            System.err.println("USAGE: java FileStore zookeeperAddresses(e.g. localhost:2181) serverId(host_port)");
            System.exit(1);
        }

        final String zkAddr = args[0];
        final String clusterName = SetupCluster.DEFAULT_CLUSTER_NAME;
        final String serverId = args[1];

        ZkClient zkClient = null;
        try
        {
            // start consumer
            final FileStore store = new FileStore(zkAddr, clusterName, serverId);

            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    System.out.println("Shutting down server: " + serverId);
                    store.disconnect();
                }
            });
            store.connect();
        }
        finally
        {
            if (zkClient != null)
            {
                zkClient.close();
            }
        }
    }
}
