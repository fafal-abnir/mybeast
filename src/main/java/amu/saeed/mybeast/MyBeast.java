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
    private static final Logger logger = LoggerFactory.getLogger(MyBeast.class);
    private final int numShards;
    List<MysqlStore> mysqlStores = new ArrayList<>();

    public MyBeast(BeastConf conf) {
        numShards = conf.getMysqlConnections().size();
        Preconditions.checkArgument(numShards > 0, "Mysql shards cannot be zero!");
        try {
            for (String conStr : conf.getMysqlConnections())
                mysqlStores.add(new MysqlStore(conStr));
        } catch (SQLException e) {
            logger.error("Error initializing MySql", e);
            System.exit(1);
        }
    }

    public MysqlStore getMysqlShard(long key) {
        int sipHash = Hashing.sipHash24().hashString(Long.toString(key), Charset.forName("UTF8")).asInt();
        int shardNum = sipHash % numShards >= 0 ? sipHash % numShards : sipHash % numShards + numShards;
        return mysqlStores.get(shardNum);
    }

    public void put(long key, byte[] val) throws SQLException {
        MysqlStore mysqlStore = getMysqlShard(key);
        mysqlStore.put(key, val);
    }

    public byte[] get(long key) throws SQLException {
        MysqlStore mysqlStore = getMysqlShard(key);
        return mysqlStore.get(key);
    }

    public void delete(long key) throws SQLException {
        MysqlStore mysqlStore = getMysqlShard(key);
        mysqlStore.delete(key);
    }

    public void close() throws SQLException {
        for (MysqlStore mysqlStore : mysqlStores)
            mysqlStore.close();
    }

    public void purge() throws SQLException {
        for (MysqlStore mysqlStore : mysqlStores)
            mysqlStore.purge();
    }

    public long size() throws SQLException {
        long sum = 0;
        for (MysqlStore mysqlStore : mysqlStores)
            sum += mysqlStore.size();
        return sum;
    }

    public long approximatedSize() throws SQLException {
        long sum = 0;
        for (MysqlStore mysqlStore : mysqlStores)
            sum += mysqlStore.approximatedSize();
        return sum;
    }

}
