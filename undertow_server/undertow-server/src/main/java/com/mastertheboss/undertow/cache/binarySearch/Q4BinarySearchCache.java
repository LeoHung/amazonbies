package com.mastertheboss.undertow.cache.binarySearch;

import com.mastertheboss.undertow.cache.BinarySearchCache;


import java.util.TreeMap;
import java.util.Vector;
import java.io.*;


public class Q4BinarySearchCache extends BinarySearchCache<String, Vector<String>>{
    public Q4BinarySearchCache(String filename){
        StringComparator strComparator = new StringComparator();
        TreeMap<String, Vector<String>> sortedMap = new TreeMap<String, Vector<String>>(strComparator);

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
                if(!sortedMap.containsKey(locationDate)){
                    sortedMap.put(locationDate, new Vector<String>());
                }
                Vector<String> l = sortedMap.get(locationDate);
                if( (rank-1) >= l.size()){
                    l.setSize(rank-1+1);
                }
                l.insertElementAt(retweet, (rank-1));
            }
        }catch(Exception e ){
            e.printStackTrace();
        }

        String[] sortedKey = (String[]) sortedMap.keySet().toArray(new String[sortedMap.keySet().size()]);
        Vector[] sortedValue = (Vector[]) sortedMap.values().toArray(new Vector[sortedMap.values().size()]);

        this.set(sortedKey, sortedValue, strComparator);

    }

}
