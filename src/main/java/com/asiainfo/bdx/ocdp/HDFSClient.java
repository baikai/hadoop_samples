package com.asiainfo.bdx.ocdp;

import java.net.URI;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by baikai on 9/1/16.
 */
public class HDFSClient {

    private String hdfsRPCUrl;

    private DistributedFileSystem dfs;

    private Configuration conf;

    private UserGroupInformation ugi;

    private static final FsPermission FS_USER_PERMISSION = new FsPermission(FsAction.ALL, FsAction.NONE,
            FsAction.NONE);

    public HDFSClient(String namenode, String port, String user){
        this.dfs = new DistributedFileSystem();
        this.conf = new Configuration();
        this.hdfsRPCUrl =  "hdfs://" + namenode + ":" + port;
        this.ugi = UserGroupInformation.createRemoteUser(user);
    }

    /**
     * Create HDFS folder.
     * @param folderPath
     * @throws Exception
     */
    public void createFolder(final String folderPath) throws Exception{
        try{
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                public Void run() throws Exception{
                    try{
                        dfs.initialize(URI.create(hdfsRPCUrl), conf);
                        dfs.mkdirs(new Path(folderPath), FS_USER_PERMISSION);
                    }catch (Exception e){
                        e.printStackTrace();
                        throw e;
                    } finally {
                        dfs.close();
                    }
                    return null;
                }
            });
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * Delete HDFS folder.
     * @param folderPath
     * @throws Exception
     */
    public void deleteFolder(final String folderPath) throws Exception{
        try{
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                public Void run() throws Exception{
                    try{
                        dfs.initialize(URI.create(hdfsRPCUrl), conf);
                        dfs.delete(new Path(folderPath), true);
                    }catch (Exception e){
                        e.printStackTrace();
                        throw e;
                    } finally {
                        dfs.close();
                    }
                    return null;
                }
            });
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * Set quota for HDFS folder.
     * @param folderPath
     * @param nameSpaceQuota
     * @param storageSpaceQuota
     * @throws Exception
     */
    public void setQuota(final String folderPath, final long nameSpaceQuota, final long storageSpaceQuota) throws Exception{
        try{
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                public Void run() throws Exception{
                    try{
                        dfs.initialize(URI.create(hdfsRPCUrl), conf);
                        dfs.setQuota(new Path(folderPath), nameSpaceQuota, storageSpaceQuota);
                    }catch (Exception e){
                        e.printStackTrace();
                        throw e;
                    } finally {
                        dfs.close();
                    }
                    return null;
                }
            });
        } catch (Exception e){
            e.printStackTrace();
        }
    }

}
