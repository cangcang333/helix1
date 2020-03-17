package com.danial.cluster;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
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
        }
    }
}
