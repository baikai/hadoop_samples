package com.asiainfo.bdx.ocdp;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by baikai on 9/1/16.
 */
public class HiveClient {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    private Connection conn;

    private String hiveJDBCUrl;

    private String superUser;

    public HiveClient(String hiveHost, String hivePort, String user){
        this.hiveJDBCUrl = "jdbc:hive2://" + hiveHost + ":" + hivePort + "/default";
        this.superUser = user;
    }

    /**
     * Create Hive database.
     * @param databaseName
     * @throws Exception
     */
    public void createDataBase(String databaseName) throws Exception{
        try{
            Class.forName(this.driverName);
            this.conn = DriverManager.getConnection(this.hiveJDBCUrl, this.superUser, "");
            Statement stmt = conn.createStatement();
            stmt.execute("create database " + databaseName);
        }catch (ClassNotFoundException e){
            e.printStackTrace();
            throw e;
        }
        catch(SQLException e){
            e.printStackTrace();
            throw e;
        }finally {
            conn.close();
        }
    }

    /**
     * Delete Hive database.
     * @param databaseName
     * @throws Exception
     */
    public void deleteDataBase(String databaseName) throws Exception{
        try{
            Class.forName(this.driverName);
            this.conn = DriverManager.getConnection(this.hiveJDBCUrl, this.superUser, "");
            Statement stmt = conn.createStatement();
            stmt.execute("drop database " + databaseName);
        }catch (ClassNotFoundException e){
            e.printStackTrace();
            throw e;
        }catch (SQLException e){
            e.printStackTrace();
            throw e;
        }finally {
            conn.close();
        }
    }
}
