package amu.saeed.mybeast;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class MyBeastClient {
    private static final Logger logger = LoggerFactory.getLogger(MyBeastClient.class);
    private ConsistentSharder<MysqlStore> mysqlShards = new ConsistentSharder<>();
    private ExecutorService threadPool;

    public MyBeastClient(BeastConf conf) throws SQLException {
        Preconditions.checkArgument(conf.getMysqlConnections().size() > 0,
                                    "The number of shards of Mysql cannot be zero!");
        for (String conStr : conf.getMysqlConnections())
            mysqlShards.addShard(new MysqlStore(conStr));
        threadPool = Executors.newFixedThreadPool(mysqlShards.numShards());
    }

    public void put(long key, byte[] val) throws SQLException {
        MysqlStore mysqlStore = mysqlShards.getShardForKey(key);
        mysqlStore.put(key, val);
    }

    public Optional<byte[]> get(long key) throws SQLException {
        MysqlStore mysqlStore = mysqlShards.getShardForKey(key);
        return mysqlStore.get(key);
    }

    public Map<Long, byte[]> multiGet(long... keys) throws InterruptedException, SQLException {
        // This is the final result to return
        Map<Long, byte[]> resultSet = new HashMap<>();

        Map<Long, Callable<Optional<byte[]>>> callables = new HashMap<>();
        for (long key : keys)
            callables.put(key, () -> mysqlShards.getShardForKey(key).get(key));

        List<Future<Optional<byte[]>>> futures = threadPool.invokeAll(callables.values(), 500,
                                                                      TimeUnit.MILLISECONDS);
        for (Future<Optional<byte[]>> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        // Here there is a simple logic:
        //    We group the keys using their corresponding shards and query all shards concurrently
        final Map<MysqlStore, List<Long>> shardsToQuery = new HashMap<>();
        for (long key : keys) {
            MysqlStore destShard = mysqlShards.getShardForKey(key);
            if (!shardsToQuery.containsKey(destShard))
                shardsToQuery.put(destShard, new ArrayList<>());
            shardsToQuery.get(destShard).add(key);
        }

        final Map<Long, SQLException> exceptions = new HashMap<>();
        final List<Runnable> tasks = shardsToQuery.entrySet().stream().filter(
                t -> t.getValue().size() > 0).map(t -> (Runnable) () -> {
            MysqlStore shard = t.getKey();
            for (long key : shardsToQuery.get(shard)) {
                try {
                    Optional<byte[]> val = shard.get(key);
                    if (val.isPresent())
                        resultSet.put(key, val.get());
                } catch (SQLException e) {
                    exceptions.put(key, e);
                }
            }
        }).collect(Collectors.toList());

        List<Callable<Map<Long, byte[]>>> tt = shardsToQuery.entrySet().stream().filter(
                t -> t.getValue().size() > 0).map(t -> (Callable<Map<Long, byte[]>>) () -> {
            Map<Long, byte[]> map = new HashMap<>();
            MysqlStore shard = t.getKey();
            for (long key : shardsToQuery.get(shard)) {
                try {
                    Optional<byte[]> val = shard.get(key);
                    if (val.isPresent())
                        map.put(key, val.get());
                } catch (SQLException e) {
                    exceptions.put(key, e);
                }
            }
            return map;
        }).collect(Collectors.toList());

        List<Future<Map<Long, byte[]>>> res = threadPool.invokeAll(tt);
        for (Future<Map<Long, byte[]>> resultFromShard : res) {
            try {
                resultSet.putAll(resultFromShard.get());
            } catch (ExecutionException e) {
            }
        }

        Thread[] queryThreads = new Thread[tasks.size()];
        for (int i = 0; i < queryThreads.length; i++) {
            queryThreads[i] = new Thread(tasks.get(i));
            queryThreads[i].start();
        }

        for (Thread queryThread : queryThreads)
            queryThread.join();

        if (exceptions.size() > 0)
            throw exceptions.values().iterator().next();

        return resultSet;
    }

    public boolean delete(long key) throws SQLException {
        MysqlStore mysqlStore = mysqlShards.getShardForKey(key);
        return mysqlStore.delete(key);
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
