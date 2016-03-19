package amu.saeed.mybeast;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Sample {
    public static void main(String[] args) throws SQLException, IOException {

        BeastConf conf = BeastConf.readFromProperties(
                BeastConf.class.getClassLoader().getResourceAsStream("beast.properties"));

        // It can be mysql connections!!!!
        ConsistentSharder<String> shards = new ConsistentSharder<>();
        for (int i = 1; i <= 16; i++)
            shards.addShard(String.format("jdbc:mysql://mysql-%d/kv%d", i, i) +
                                    "?useUnicode=true&useConfigs=maxPerformance"
                                    + "&autoReconnect=true" +
                                    "&characterEncoding=UTF-8&user=root&password=chamran");

        String shard = shards.getShardForKey(1231321321L);

        Connection connection = null;
        CallableStatement putStatements = null;
        CallableStatement getStatements = null;
        CallableStatement delStatements = null;

        connection = DriverManager.getConnection("conString");
        // puts a  long and a blob
        putStatements = connection.prepareCall("{CALL kvput(?, ?)}");
        // returns a blob given a long key
        getStatements = connection.prepareCall("{CALL kvget(?)}");
        // deletes a blob given a long key
        delStatements = connection.prepareCall("{CALL kvdel(?)}");

        // before put
        byte[] bb = GZip4Persian.compressAndFit(new String("JSON"), 65000);

        // after get
        GZip4Persian.uncompress(bb);
    }
}
