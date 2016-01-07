package amu.saeed.mybeast;

import com.google.common.base.Preconditions;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class MysqlStore {
    private final Object connectionLock = new Object();
    private Connection connections = null;
    private CallableStatement putStatements = null;
    private CallableStatement getStatements = null;
    private CallableStatement delStatements = null;

    public MysqlStore(String conString) throws SQLException {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        connections = DriverManager.getConnection(conString);
        putStatements = connections.prepareCall("{CALL kvput(?, ?)}");
        getStatements = connections.prepareCall("{CALL kvget(?)}");
        delStatements = connections.prepareCall("{CALL kvdel(?)}");
    }

    public synchronized void put(long key, byte[] val) throws SQLException {
        Preconditions.checkArgument(val.length <= 65535, "The length of value must be smaller than 65535");
        synchronized (connectionLock) {
            putStatements.setLong(1, key);
            putStatements.setBytes(2, val);
            putStatements.executeUpdate();
        }
    }

    public byte[] get(long key) throws SQLException {
        synchronized (connectionLock) {
            getStatements.setLong(1, key);
            boolean hasResult = getStatements.execute();
            if (hasResult) {
                ResultSet result = getStatements.getResultSet();
                if (result.next()) {
                    long l = result.getLong(1);
                    byte[] val = result.getBytes(2);
                    return val;
                }
            }
        }
        return null;
    }

    public void delete(long key) throws SQLException {
        synchronized (connectionLock) {
            delStatements.setLong(1, key);
            delStatements.executeUpdate();
        }
    }

    public void purge() throws SQLException {
        synchronized (connectionLock) {
            connections.prepareCall("{ CALL trunc_all() }").execute();
        }
    }

    public void close() throws SQLException {
        connections.close();
    }

    public Map<Long, byte[]> getAll() throws SQLException {
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
        synchronized (connectionLock) {
            connections.setAutoCommit(false);
            connections.commit();
            connections.setAutoCommit(true);
        }
    }

    public int size() throws SQLException {
        synchronized (connectionLock) {
            ResultSet res = connections.createStatement().executeQuery("SELECT size();");
            res.next();
            return res.getInt(1);
        }
    }

    public int approximatedSize() throws SQLException {
        synchronized (connectionLock) {
            ResultSet res = connections.createStatement().executeQuery("SELECT approx_size();");
            res.next();
            return res.getInt(1);
        }
    }
}
