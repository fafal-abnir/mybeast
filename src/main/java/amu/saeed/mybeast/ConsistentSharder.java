package amu.saeed.mybeast;

import com.google.common.hash.Hashing;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ConsistentSharder <T> implements Iterable<T> {
    List<T> shards = new ArrayList<>();

    public T getShardForKey(long key) {
        int crc = Hashing.crc32().hashString(Long.toString(key), Charset.forName("UTF8")).asInt();
        int shardNum = crc % shards.size() >= 0 ?
                       crc % shards.size() :
                       crc % shards.size() + shards.size();
        return shards.get(shardNum);
    }

    public void addShard(T shard, int repeats) {
        for (int i = 0; i < repeats; i++)
            shards.add(shard);
    }

    public void addShard(T shard) {
        addShard(shard, 1);
    }

    @Override
    public Iterator<T> iterator() {
        return shards.iterator();
    }
}
