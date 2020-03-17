package com.danial.cluster;

import org.apache.helix.HelixManager;
import org.apache.helix.participant.statemachine.StateModelFactory;

public class FileStoreStateModelFactory extends StateModelFactory<FileStoreStateModel> {
    private final HelixManager manager;

    public FileStoreStateModelFactory(HelixManager manager)
    {
        this.manager = manager;
    }

    @Override
    public FileStoreStateModel createNewStateModel(String resource, String partition)
    {
        FileStoreStateModel model;
        model = new FileStoreStateModel(manager, partition.split("_")[0], partition);
        return model;
    }
}
