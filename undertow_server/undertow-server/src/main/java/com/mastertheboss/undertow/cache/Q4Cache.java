
package com.mastertheboss.undertow.cache;

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

public class Q4Cache{

    class LocationTime{
        public int locationId;
        public long time;
        public LocationTime(int locationId, long time){
            this.locationId = locationId; this.time = time;
        }
        public boolean equals(Object obj) {
            LocationTime lt2 = (LocationTime) obj;
            if(lt2.locationId == this.locationId && lt2.time == this.time){
                return true;
            }
            return false;
        }
        public int hashCode(){
            return (int)locationId + (int)time;
        }
    }
    class TagIDTweetids{
        public int tagId;
        public ArrayList<Long> tweetids;
        public TagIDTweetids(int tagId, ArrayList<Long> tweetids){
            this.tagId = tagId;
            this.tweetids = tweetids;
        }
    }

    // location, time -> array of tag, tweetid
    int maxLocationId=0;
    int maxTagId= 0;
    private ConcurrentMap<String, Integer> location2locationId;
    private ConcurrentMap<Integer, String> tagId2tag;
    private ConcurrentMap<String, Integer> tag2tagId;
    private ConcurrentMap<LocationTime, Vector<TagIDTweetids>> locationTime2tagIDTweetids;

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    public Q4Cache(){
        location2locationId = new ConcurrentHashMap<String, Integer>();
        tagId2tag = new ConcurrentHashMap<Integer, String>();
        tag2tagId = new ConcurrentHashMap<String, Integer>();
        locationTime2tagIDTweetids = new ConcurrentHashMap<LocationTime, Vector<TagIDTweetids>>();
    }

    public int size(){
        return locationTime2tagIDTweetids.size();
    }

    public Integer getLocationId(String location){
        Integer locationId = location2locationId.get(location);
        if(locationId == null){
            locationId = maxLocationId;
            location2locationId.put(location, maxLocationId);
            maxLocationId++;
        }
        return locationId;
    }

    public String getTag(int tagId){
        String tag = tagId2tag.get(tagId);
        return tag;
    }

    public Integer getTagId(String tag){
        Integer tagId = tag2tagId.get(tag);
        if(tagId == null){
            tagId = maxTagId;
            tagId2tag.put(maxTagId, tag);
            tag2tagId.put(tag, maxTagId);
            maxTagId++;
        }
        return tagId;
    }

    public long getTime(String time){
        long timeDate = -1;
        try{
            timeDate = sdf.parse(time).getTime();
        }catch(Exception e){
            e.printStackTrace();
        }
        return timeDate;
    }

    public void put(String location, String time, String rankStr, String tagTweetids){
        int rank = Integer.parseInt(rankStr) ;
        // get location id
        Integer locationId = getLocationId(location);

        // convert time => time.sec
        long timeDate = getTime(time);

        // convert tag:id => tagid, Array(id)
        String[] tmp = tagTweetids.split(":");
        String tag = tmp[0];
        String[] tweetidStrs = tmp[1].split(",");

        Integer tagId = getTagId(tag);

        ArrayList<Long> tweetids = new ArrayList<Long>();
        for(String tweetidStr: tweetidStrs){
            long tweetid = Long.parseLong(tweetidStr);
            tweetids.add(tweetid);
        }

        TagIDTweetids tagIDTweetids = new TagIDTweetids(tagId, tweetids);
        LocationTime locationTime = new LocationTime(locationId, timeDate);

        if(!locationTime2tagIDTweetids.containsKey(locationTime)){
            locationTime2tagIDTweetids.put(locationTime, new Vector<TagIDTweetids>());
        }

        Vector<TagIDTweetids> v_TagIDTweetids = locationTime2tagIDTweetids.get(locationTime);
        if(v_TagIDTweetids.size() <= rank-1 ){
            v_TagIDTweetids.setSize(rank-1+1);
        }
        v_TagIDTweetids.add(rank-1, tagIDTweetids);

    }

    public String get(String location, String timeStr, String mStr, String nStr){
        int locationId = getLocationId(location);
        long time = getTime(timeStr);
        int m = Integer.parseInt(mStr);
        int n = Integer.parseInt(nStr);

        Vector<TagIDTweetids> tagTweetidsVector =
                locationTime2tagIDTweetids.get(new LocationTime(locationId, time));


        if(tagTweetidsVector == null) return null;

        StringBuilder sb = new StringBuilder();

        for(int i = m-1 ; i <= n-1; i++){
            if( i >= tagTweetidsVector.size()){
                sb.append("null\n");
                continue;
            }
            TagIDTweetids tagTweetids = tagTweetidsVector.get(i);
            if(tagTweetids==null){
                sb.append("null\n");
                continue;
            }

            String tag = getTag(tagTweetids.tagId);
            sb.append(tag);
            sb.append(":");
            for( int j = 0 ; j < tagTweetids.tweetids.size() ; j++){
                sb.append(tagTweetids.tweetids.get(j));
                if(i < tagTweetids.tweetids.size() -1){
                    sb.append(',');
                }
            }
            sb.append("\n");
        }

        return sb.toString();
    }

}
