package amu.saeed.mybeast;

import java.sql.SQLException;

public class Main {
    public static void main(String[] args) throws SQLException, InterruptedException {
        BeastConf conf = new BeastConf();
        for (int i = 1; i <= 16; i++)
            conf.addMysqlShard(String.format("jdbc:mysql://mysql-%d/kv%d", i, i) +
                                       "?useUnicode=true&useConfigs=maxPerformance" +
                                       "&characterEncoding=UTF-8&user=root&password=chamran");

        MyBeastClient beast = new MyBeastClient(conf);
        while (true) {
            System.out.printf("%,d\n", beast.size());
            Thread.sleep(1000);
        }
    }

}
