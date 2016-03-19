package amu.saeed.mybeast;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class BeastConf implements Serializable {
    public static final String REDIS_ADDRESS_PROP = "redis_url";
    public static final String MYSQL_ADDRESSES_PROP = "mysql_jdbc_urls";

    String redisAddressUrl;
    List<String> mysqlConnections = new ArrayList<>();

    public static BeastConf readFromProperties(InputStream stream) throws IOException {
        Properties prop = new Properties();
        prop.load(stream);
        BeastConf conf = new BeastConf();
        conf.setRedisAddressUrl(prop.getProperty(REDIS_ADDRESS_PROP, ""));
        String[] mysql_urls = prop.getProperty(MYSQL_ADDRESSES_PROP).split(",");
        Preconditions.checkArgument(mysql_urls.length > 0,
                                    "The number of mysql shards must be " + "greater than zero.");
        conf.mysqlConnections = Arrays.asList(mysql_urls);
        return conf;
    }

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
