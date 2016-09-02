package com.asiainfo.bdx.ocdp;

/**
 * Created by baikai on 9/1/16.
 */
public class HadoopSamplesTest {

    public static final void main(String[] args){

        HDFSClient hdfsClient = new HDFSClient("hdfs namenode ip/hostname", "rpc port", "hdfs super user");
        try{
            System.out.println("Creare hdfs folder.");
            hdfsClient.createFolder("/testhdfs");
            System.out.println("Set namespace and storage space quota.");
            hdfsClient.setQuota("/testhdfs", 1000, 100000);
            System.out.println("Delete hdfs folder.");
            hdfsClient.deleteFolder("/testhdfs");
        }catch (Exception e){
            e.printStackTrace();
        }

        HiveClient hiveClient = new HiveClient("hive server2 ip/hostname", "10000", "hive user", "");
        try{
            System.out.println("Create hive database.");
            hiveClient.createDataBase("testhive");
            System.out.println("Delete hive database.");
            hiveClient.deleteDataBase("testhive");
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
