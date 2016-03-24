package amu.saeed.mybeast;

import com.google.common.base.Preconditions;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A key-value store warpper around MySQL. The key is always a 64-bit long and
 * the value is a byte array which its length must be less than
 * {@link MysqlStore#MAX_VALUE_LEN}. <br>
 * {@link MysqlStore} provides three basic methods:
 * <ul>
 * <li>put</li>
 * <li>get</li>
 * <li>delete</li>
 * </ul>
 */
public class MysqlStore {
    public static final int QUERY_TIMEOUT_SECONDS = 1;
    public static final int MAX_VALUE_LEN = 65500;
    private final Object connectionLock = new Object();
    private final boolean isDummy;
    private Connection connections = null;
    private CallableStatement putStatement = null;
    private CallableStatement getStatement = null;
    private CallableStatement delStatement = null;

    /**
     * Builds a MysqlStore given a JDBC connection string.
     *
     * @param conString the JDBC connection string.
     * @throws SQLException if it cannot connect.
     */
    public MysqlStore(String conString) throws SQLException {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        connections = DriverManager.getConnection(conString);
        putStatement = connections.prepareCall("{CALL kvput(?, ?)}");
        putStatement.setQueryTimeout(1);
        getStatement = connections.prepareCall("{CALL kvget(?)}");
        getStatement.setQueryTimeout(1);
        delStatement = connections.prepareCall("{CALL kvdel(?)}");
        delStatement.setQueryTimeout(1);
        isDummy = false;
    }

    private MysqlStore() {
        isDummy = true;
    }

    public static MysqlStore createDummy() {
        return new MysqlStore();
    }

    /**
     * Inserts a key-val to mysql.
     *
     * @param key
     * @param val
     * @throws SQLException             if it cannot insert to mysql.
     * @throws IllegalArgumentException if the length of value is larger than
     *                                  {@link MysqlStore#MAX_VALUE_LEN}
     */
    public void put(long key, byte[] val) throws SQLException {
        Preconditions.checkArgument(val.length <= MAX_VALUE_LEN,
                                    "The length of value must be smaller than " + MAX_VALUE_LEN);
        if (isDummy)
            throw new SQLException("The shard is in dummy mode.");

        synchronized (connectionLock) {
            putStatement.setLong(1, key);
            putStatement.setBytes(2, val);
            putStatement.executeUpdate();
        }
    }

    /**
     * Retrieves the value given a key.
     *
     * @param key
     * @return an Optional byte array; empty if the key does not exist.
     * @throws SQLException
     */
    public Optional<byte[]> get(long key) throws SQLException {
        if (isDummy)
            throw new SQLException("The shard is in dummy mode.");

        synchronized (connectionLock) {
            getStatement.setLong(1, key);
            boolean hasResult = getStatement.execute();
            if (hasResult) {
                ResultSet result = getStatement.getResultSet();
                if (result.next()) {
                    long l = result.getLong(1);
                    byte[] val = result.getBytes(2);
                    return Optional.of(val);
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Deletes a key-val given the key.
     *
     * @param key
     * @return true if the key was deleted; otherwise false i.e. the key was
     * not present.
     * @throws SQLException
     */
    public boolean delete(long key) throws SQLException {
        if (isDummy)
            throw new SQLException("The shard is in dummy mode.");

        synchronized (connectionLock) {
            delStatement.setLong(1, key);
            return delStatement.executeUpdate() > 0;
        }
    }

    /**
     * Clears all of the data from database.
     *
     * @throws SQLException
     */
    public void purge() throws SQLException {
        if (isDummy)
            throw new SQLException("The shard is in dummy mode.");

        synchronized (connectionLock) {
            connections.prepareCall("{ CALL trunc_all() }").execute();
        }
    }

    public void close() throws SQLException {
        if (isDummy)
            throw new SQLException("The shard is in dummy mode.");

        connections.close();
    }

    public Map<Long, byte[]> getAll() throws SQLException {
        if (isDummy)
            throw new SQLException("The shard is in dummy mode.");

        Map<Long, byte[]> map = new HashMap<>();
        synchronized (connectionLock) {
            CallableStatement stm = connections.prepareCall("{ CALL get_all() }");
            boolean hasResult = stm.execute();
            if (hasResult) {
                ResultSet result = stm.getResultSet();
                while (result.next()) {
                    long l = result.getLong(1);
                    byte[] val = result.getBytes(2);
                    map.put(l, val);
                }
            }
        }
        return map;
    }

    public void commit() throws SQLException {
        if (isDummy)
            throw new SQLException("The shard is in dummy mode.");

        synchronized (connectionLock) {
            connections.setAutoCommit(false);
            connections.commit();
            connections.setAutoCommit(true);
        }
    }

    public int size() throws SQLException {
        if (isDummy)
            throw new SQLException("The shard is in dummy mode.");

        synchronized (connectionLock) {
            ResultSet res = connections.createStatement().executeQuery("SELECT size();");
            res.next();
            return res.getInt(1);
        }
    }

    public int approximatedSize() throws SQLException {
        if (isDummy)
            throw new SQLException("The shard is in dummy mode.");

        synchronized (connectionLock) {
            ResultSet res = connections.createStatement().executeQuery("SELECT approx_size();");
            res.next();
            return res.getInt(1);
        }
    }
}
