package amu.saeed.mybeast;

import com.google.common.base.Stopwatch;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MysqlStoreTest {

    MysqlStore mysqlStore;

    @Before
    public void before() throws SQLException {
        mysqlStore = new MysqlStore("jdbc:mysql://mysql-1/testkv?useUnicode=true&useConfigs=maxPerformance&"
                + "characterEncoding=UTF-8&user=root&password=chamran");
        mysqlStore.purge();
    }

    @After
    public void after() throws SQLException {
        mysqlStore.commit();
        mysqlStore.purge();
        mysqlStore.close();
    }

    @Test
    public void testOps() throws SQLException {
        Map<Long, byte[]> map = new HashMap<>();
        map.put(1L, "BIR".getBytes());
        map.put(2L, "IKI".getBytes());
        map.put(3L, "OOCH".getBytes());

        // check equals
        for (Map.Entry<Long, byte[]> entry : map.entrySet())
            mysqlStore.put(entry.getKey(), entry.getValue());
        mysqlStore.commit();
        assertMapsEqual(map, mysqlStore.getAll());
        Assert.assertArrayEquals(mysqlStore.get(1L), map.get(1L));

        // check absent key
        Assert.assertNull(mysqlStore.get(20L));

        // check delete
        mysqlStore.delete(2L);
        Assert.assertNull(mysqlStore.get(2L));
        map.remove(2L);
        mysqlStore.commit();
        assertMapsEqual(map, mysqlStore.getAll());

        // check delete absent key
        mysqlStore.delete(20L);
        assertMapsEqual(map, mysqlStore.getAll());

        // put twice
        mysqlStore.put(1L, "BIR".getBytes());
        mysqlStore.put(1L, "BIR".getBytes());
        assertMapsEqual(map, mysqlStore.getAll());
    }


    @Test
    public void pressureTest() throws SQLException {
        final Map<Long, byte[]> map = new HashMap<>();
        Random random = new Random();
        Executors.newSingleThreadScheduledExecutor()
                .scheduleWithFixedDelay(() -> System.out.printf("Inserted %,d\n", map.size()), 0, 1,
                        TimeUnit.SECONDS);

        Stopwatch stopwatch = Stopwatch.createStarted();
        int INSERT_COUNT = 10_000;
        for (int i = 0; i < INSERT_COUNT; i++) {
            Long l = random.nextLong();
            mysqlStore.put(l, Long.toBinaryString(l).getBytes());
            map.put(l, Long.toBinaryString(l).getBytes());
        }
        mysqlStore.commit();
        System.out.printf("Rate: %,d qps\n", INSERT_COUNT / stopwatch.elapsed(TimeUnit.SECONDS));
        assertMapsEqual(map, mysqlStore.getAll());
    }

    private void assertMapsEqual(Map<Long, byte[]> map1, Map<Long, byte[]> map2) {
        Assert.assertEquals(map1.size(), map2.size());
        for (Map.Entry<Long, byte[]> entry : map1.entrySet())
            Assert.assertArrayEquals(map2.get(entry.getKey()), entry.getValue());
    }
}
