package com.mastertheboss.undertow.connection;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;

public class DataSource {

    private static DataSource datasource;
    private BasicDataSource ds;

    private DataSource(String ip, String username, String password, String dbname) throws IOException, SQLException, PropertyVetoException {
        ds = new BasicDataSource();
        ds.setDriverClassName("com.mysql.jdbc.Driver");
        ds.setUsername(username);
        ds.setPassword(password);
        ds.setUrl("jdbc:mysql://"+ip+"/"+dbname);
       
     // the settings below are optional -- dbcp can work with defaults
        ds.setInitialSize(100);
        ds.setMaxIdle(100);
        ds.setMinIdle(5);
        //ds.setMaxOpenPreparedStatements(180);
    }

    public static void init(String ip, String username, String password, String dbname ) throws Exception {
        datasource = new DataSource( ip, username, password, dbname);
    }

    public static DataSource getInstance() throws IOException, SQLException, PropertyVetoException {
        /*
        if (datasource == null) {
            datasource = new DataSource();
            return datasource;
        } else {
            return datasource;
        }*/
        return datasource ;
    }

    public Connection getConnection() throws SQLException {
        return this.ds.getConnection();
    }

}
