package amu.saeed.mybeast;

import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MyBeast {
    public static final int MAX_BEAST_BLOB_SIZE = 65_535;
    private static final Logger logger = LoggerFactory.getLogger(MyBeast.class);
    ConsistentSharder<MysqlStore> mysqlShards = new ConsistentSharder<>();

    public MyBeast(BeastConf conf) {
        Preconditions.checkArgument(conf.getMysqlConnections().size() > 0, "Mysql shards cannot be zero!");
        try {
            for (String conStr : conf.getMysqlConnections())
                mysqlShards.addShard(new MysqlStore(conStr));
        } catch (SQLException e) {
            logger.error("Error initializing MySql", e);
            System.exit(1);
        }
    }

    public void put(long key, byte[] val) throws SQLException {
        MysqlStore mysqlStore = mysqlShards.getShardForKey(key);
        mysqlStore.put(key, val);
    }

    public byte[] get(long key) throws SQLException {
        MysqlStore mysqlStore = mysqlShards.getShardForKey(key);
        return mysqlStore.get(key);
    }

    public void delete(long key) throws SQLException {
        MysqlStore mysqlStore = mysqlShards.getShardForKey(key);
        mysqlStore.delete(key);
    }

    public void close() throws SQLException {
        for (MysqlStore mysqlStore : mysqlShards)
            mysqlStore.close();
    }

    public void purge() throws SQLException {
        for (MysqlStore mysqlStore : mysqlShards)
            mysqlStore.purge();
    }

    public long size() throws SQLException {
        long sum = 0;
        for (MysqlStore mysqlStore : mysqlShards)
            sum += mysqlStore.size();
        return sum;
    }

    public long approximatedSize() throws SQLException {
        long sum = 0;
        for (MysqlStore mysqlStore : mysqlShards)
            sum += mysqlStore.approximatedSize();
        return sum;
    }

}
