package com.danial.cluster;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class CheckpointFile {
    private static final String TEMP = ".bak";
    private static String CHECK_POINT_FILE = "lastprocessedchange.checkpoint";

    private String checkpointDirPath;

    public CheckpointFile(String checkpointDirPath) throws IOException {
        this.checkpointDirPath = checkpointDirPath;
        File checkpointdir = new File(checkpointDirPath);
        if (!checkpointdir.exists() && !checkpointdir.mkdirs())
        {
            throw new IOException("Unable to create SCN file parent: " + checkpointdir.getAbsolutePath());
        }
    }

    public void checkpoint(ChangeRecord lastRecordProcessed) throws IOException {
        // delete the temp file if one exists
        File tempCheckpointFile = new File(checkpointDirPath + "/" + CHECK_POINT_FILE + TEMP);
        if (tempCheckpointFile.exists() && !tempCheckpointFile.delete())
        {
            System.err.println("Unable to erase temp SCN file: " + tempCheckpointFile.getAbsolutePath());
        }

        String checkpointFileName = checkpointDirPath + "/" + CHECK_POINT_FILE;
        File checkpointfile = new File(checkpointFileName);
        if (checkpointfile.exists() && !checkpointfile.renameTo(tempCheckpointFile))
        {
            System.err.println("Unable to backup SCN file");
        }

        if (!checkpointfile.createNewFile())
        {
            System.err.println("Unable to create new SCN file: " + checkpointfile.getAbsolutePath());
        }

        FileWriter writer = new FileWriter(checkpointfile);
        writer.write(lastRecordProcessed.toString());
        writer.flush();
        writer.close();
        System.out.println("scn persisted: " + lastRecordProcessed.txid);
    }

    public ChangeRecord findLastRecordProcessed()
    {
        String checkpointFileName = checkpointDirPath + "/" + CHECK_POINT_FILE;
        File file = new File(checkpointFileName);
        ChangeRecord record = null;
        if (file.exists())
        {
            try
            {
                String line = FileUtils.readFileToString(file);
                record = ChangeRecord.fromString(line);
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }

        return record;
    }
}
