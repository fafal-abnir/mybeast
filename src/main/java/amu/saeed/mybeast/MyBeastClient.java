package amu.saeed.mybeast;

import com.google.common.base.Preconditions;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

public class MyBeastClient {
    private static final int MULTI_GET_TIMEOUT_MILLIS = 100;
    private ConsistentSharder<MysqlStore> mysqlShards = new ConsistentSharder<>();
    private Map<MysqlStore, ExecutorService> queryThreads;

    public MyBeastClient(BeastConf conf) {
        Preconditions.checkArgument(conf.getMysqlConnections().size() > 0,
                                    "The number of shards of Mysql cannot be zero!");

        int numDummies = 0;
        for (String conStr : conf.getMysqlConnections())
            try {
                mysqlShards.addShard(new MysqlStore(conStr));
            } catch (SQLException e) {
                mysqlShards.addShard(MysqlStore.createDummy());
                numDummies++;
            }

        Preconditions.checkState(numDummies < mysqlShards.numShards(),
                                 "All of the shards got error during connection.");

        queryThreads = new HashMap<>(mysqlShards.numShards());
        for (MysqlStore mysqlShard : mysqlShards)
            queryThreads.put(mysqlShard, Executors.newSingleThreadScheduledExecutor());
    }

    public void put(long key, byte[] val) throws SQLException {
        MysqlStore mysqlStore = mysqlShards.getShardForKey(key);
        mysqlStore.put(key, val);
    }

    public Optional<byte[]> get(long key) throws SQLException {
        MysqlStore mysqlStore = mysqlShards.getShardForKey(key);
        return mysqlStore.get(key);
    }

    public synchronized MultiGetResult multiGet(long... keys) throws InterruptedException {
        final MultiGetResult result = new MultiGetResult();

        // build a Runnable per key which will be executed asyncly
        Map<Long, Runnable> querieTasks = new HashMap<>();
        for (long key : keys)
            querieTasks.put(key, () -> {
                try {
                    Optional<byte[]> val = mysqlShards.getShardForKey(key).get(key);
                    if (val.isPresent())
                        result.presents.put(key, val.get());
                    else
                        result.absents.add(key);
                } catch (SQLException e) {
                    result.exceptions.put(key, e);
                }
            });

        // submit queries to corresponding executor related its shard
        Map<Long, Future> futures = new HashMap<>();
        for (Map.Entry<Long, Runnable> entry : querieTasks.entrySet()) {
            ExecutorService executor = queryThreads.get(mysqlShards.getShardForKey(entry.getKey()));
            futures.put(entry.getKey(), executor.submit(entry.getValue()));
        }

        //
        for (Map.Entry<Long, Future> entry : futures.entrySet()) {
            try {
                entry.getValue().get(MULTI_GET_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
                throw new IllegalStateException("This block shall not reachhere: The runnable for "
                                                        + "queries shall never throw exceptions.");
            } catch (TimeoutException e) {
                entry.getValue().cancel(true);
                result.timeouts.add(entry.getKey());
            }
        }

        if (result.totalResults() != keys.length)
            throw new IllegalStateException();
        return result;
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

    public class MultiGetResult {
        final Map<Long, byte[]> presents = Collections.synchronizedMap(new HashMap<>());
        final Map<Long, Exception> exceptions = Collections.synchronizedMap(new HashMap<>());
        final Set<Long> timeouts = Collections.synchronizedSet(new HashSet<>());
        final Set<Long> absents = Collections.synchronizedSet(new HashSet<>());

        public Map<Long, Exception> getExceptions() {
            return exceptions;
        }

        public Map<Long, byte[]> getPresents() {
            return presents;
        }

        public Set<Long> getTimeouts() {
            return timeouts;
        }

        public Set<Long> getAbsents() {
            return absents;
        }

        public int totalResults() {
            return presents.size() + absents.size() + exceptions.size() + timeouts.size();
        }

        @Override
        public String toString() {
            return "MultiGetResult{" +
                    "presents=" + presents.size() +
                    ", absents=" + absents.size() +
                    ", exceptions=" + exceptions.size() +
                    ", timeouts=" + timeouts.size() +
                    '}';
        }
    }
}
