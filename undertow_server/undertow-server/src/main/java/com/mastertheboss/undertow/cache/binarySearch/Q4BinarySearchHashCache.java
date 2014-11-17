package com.mastertheboss.undertow.cache.binarySearch;

import com.mastertheboss.undertow.cache.BinarySearchCache;
import com.mastertheboss.undertow.cache.MyCache;


import java.util.TreeMap;
import java.util.Vector;
import java.io.*;


public class Q4BinarySearchHashCache implements MyCache<String,Vector<String>> {
    BinarySearchCache<Integer,Vector<String>> cache = new BinarySearchCache<Integer,Vector<String>>();

    public Q4BinarySearchHashCache(String filename){
        IntegerComparator intComparator = new IntegerComparator();
        TreeMap<Integer, Vector<String>> sortedMap = new TreeMap<Integer, Vector<String>>(intComparator);

        try{
            BufferedReader bf = new BufferedReader(new FileReader(filename));
            String line = null;

            while((line = bf.readLine()) != null){
                String[] tmp = line.split("\t");
                String locationDateRank = tmp[0];
                String retweet = tmp[1];
                String[] tmp2 = locationDateRank.split("_");
                String location ="";
                for( int i =0 ;i < tmp2.length -2 ; i++){
                    if(i >0){
                        location += "_";
                    }
                    location += tmp2[i];
                }
                String date = tmp2[tmp2.length-2];
                int rank = Integer.parseInt(tmp2[tmp2.length-1]);

                String locationDate = location +"_"+ date;
                Integer locationDateHash = locationDate.hashCode();
                if(!sortedMap.containsKey(locationDateHash)){
                    sortedMap.put(locationDateHash, new Vector<String>());
                }
                Vector<String> l = sortedMap.get(locationDateHash);
                if( (rank-1) >= l.size()){
                    l.setSize(rank-1+1);
                }
                l.insertElementAt(retweet, (rank-1));
            }
        }catch(Exception e ){
            e.printStackTrace();
        }

        Integer[] sortedKey = (Integer[]) sortedMap.keySet().toArray(new Integer[sortedMap.keySet().size()]);
        Vector<String>[] sortedValue = (Vector[]) sortedMap.values().toArray(new Vector[sortedMap.values().size()]);

        this.cache.set(sortedKey, sortedValue, intComparator);

        sortedMap = null;
        System.gc();
    }

    public Vector<String> get(String key){
        return this.cache.get(key.hashCode());
    }
    public void put(String key, Vector<String> value ){
        return ;
    }

}
