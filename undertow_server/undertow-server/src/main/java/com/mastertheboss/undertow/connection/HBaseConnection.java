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



public class HBaseConnection{

    public static HConnection getHBConnection(String hbaseIp){
        if(hbaseIp.trim().compareTo("null") ==0){return null;}

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",hbaseIp);
        //conf.setInt("hbase.htable.threads.max", 90);
        //conf.setInt("hbase.client.ipc.pool.size", 90 );
        //conf.set("hbase.client.ipc.pool.type","RoundRobin");

        HConnection connection = null;
        try{
            connection = HConnectionManager.createConnection(conf);
        }catch(Exception e){
            e.printStackTrace();
        }
        return connection;
    }
}
