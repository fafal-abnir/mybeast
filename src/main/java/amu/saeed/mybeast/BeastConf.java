package amu.saeed.mybeast;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class BeastConf implements Serializable{
    String redisAddressUrl;
    List<String> mysqlConnections = new ArrayList<>();

    public String getRedisAddressUrl() {
        return redisAddressUrl;
    }

    public void setRedisAddressUrl(String redisAddressUrl) {
        this.redisAddressUrl = redisAddressUrl;
    }

    public List<String> getMysqlConnections() {
        return mysqlConnections;
    }

    public void addMysqlShard(String conString) {
        mysqlConnections.add(conString);
    }

}
