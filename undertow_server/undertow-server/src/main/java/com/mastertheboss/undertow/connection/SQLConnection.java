package com.mastertheboss.undertow.connection;


import io.undertow.Undertow;
import io.undertow.server.*;
import io.undertow.util.Headers;
import io.undertow.*;
import io.undertow.server.handlers.*;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.math.BigInteger;
import java.util.Date;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import java.util.concurrent.*;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Vector;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;

import java.io.*;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.nio.ByteBuffer;
import org.apache.commons.codec.binary.Base64;


public class SQLConnection{
    static Connection mysqlConn=null;

    public static Connection getSQLConnection(String mySqlIp){
        if(mySqlIp.equals("null")){
            return null;
        }

        if(SQLConnection.mysqlConn == null){

            String mysql_url=mySqlIp;
            String mysql_db="tweet";
            String mysql_user="root";
            String mysql_password="password";

            String driver="com.mysql.jdbc.Driver";
            try {
                // make the connection
                Class.forName(driver);
                String jdbc_url = "jdbc:mysql://" + mysql_url + ":3306/"+mysql_db+"?useUnicode=true&characterEncoding=UTF-8";
                SQLConnection.mysqlConn = DriverManager.getConnection(jdbc_url, mysql_user, mysql_password);
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        return SQLConnection.mysqlConn;
    }
}
