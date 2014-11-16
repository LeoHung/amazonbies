package com.mastertheboss.undertow.myutils;


import io.undertow.Undertow;
import io.undertow.server.*;
import io.undertow.util.Headers;
import io.undertow.*;
import io.undertow.server.handlers.*;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.math.BigInteger;
import java.util.Date;
import java.lang.ThreadLocal;

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


public class Q2IndexConvertor{

    static String thisline;
    static final  ThreadLocal<SimpleDateFormat> fmt = new ThreadLocal<SimpleDateFormat>(){
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss");
        }
    };
    static Date originDate =null ;

    public byte[] longToBytes(long x) {
        ByteBuffer buffer = null;
        buffer = ByteBuffer.allocate( 8 );
        buffer.putLong(x);
        return buffer.array();
    }

    public static void initiateOriginDate() throws Exception
    {
        originDate = fmt.get().parse("2014-01-01+00:00:00");
    }

    public static Date getInitiateOriginDate() throws Exception{
        if(originDate == null){
            originDate = fmt.get().parse("2014-01-01+00:00:00");
        }
        return originDate;
    }

    public String convert(String text) throws Exception
    {
        String[] uidDtm = text.split("_");
        Date dt = fmt.get().parse(uidDtm[1]);
        Long seconds = (dt.getTime()-originDate.getTime())/1000;
        Long dt_uid = Long.parseLong(seconds.toString() + uidDtm[0]);
        byte[] binaryData = longToBytes(dt_uid);
        String encoded = Base64.encodeBase64String(binaryData);
        int ln = encoded.length();
        String a = encoded.substring(0,ln/2);
        String b = encoded.substring(ln/2,ln);
        return b+a;
    }

   public static Long convertToLong(String userid, String tweet_time) throws Exception
    {
       // String[] uidDtm = text.split("_");
        
        Date dt = null;
        try{
            dt = fmt.get().parse(tweet_time);
        }catch(Exception e){
            e.printStackTrace();
            System.out.println(userid  +" " + tweet_time );
            return (long) -1;
        }        

        //if(originDate ==null){
        //    initiateOriginDate();
        //}
        //System.out.println(originDate );
        //Long seconds = (dt.getTime()-originDate.getTime())/1000;
        Long seconds = (dt.getTime()-getInitiateOriginDate().getTime())/1000;
        
        try{
            Long dt_uid = Long.parseLong(seconds.toString() + userid);
            return dt_uid;
        }catch(Exception e){
            System.out.println("Converting Error: " + seconds.toString() + " " + userid);
            return (long)-1;
        }
        //return encoded;
    }


    public Q2IndexConvertor() throws Exception{
        this.initiateOriginDate();
    }

}
