package amu.saeed.mybeast;

import com.google.common.base.Stopwatch;

import java.sql.SQLException;
import java.util.Map;

public class Sample {
    public static void main(String[] args) throws SQLException, InterruptedException {
        final BeastConf beastConf = new BeastConf();
        for (int i = 1; i <= 16; i++)
            beastConf.addMysqlShard(String.format("jdbc:mysql://mysql-%d/kv%d", i, i) +
                                            "?useUnicode=true&useConfigs=maxPerformance" +
                                            "&autoReconnect=true&characterEncoding=UTF-8"
                                            + "&user=root&password=chamran");

        final MyBeastClient beast = new MyBeastClient(beastConf);

        long[] keys = new long[] {219829150337218916L, 219800869614913441L, 219840077963517002L,
                                  219839678952270132L, 219794474180033114L, 219898552462933776L,
                                  219807776349642423L, 219822140250046034L, 219890894592461477L,
                                  219846022879844994L, 219842366135182114L, 219887359949332217L,
                                  219854334871525329L, 219845969292692950L, 219868993645550452L,
                                  219854451350902168L, 219818011085944777L, 219868187858387716L,
                                  219833948693719320L, 219843281657143997L};
        beast.multiGet(keys);

        keys = new long[] {244236721246914296L, 244256545575161799L, 244283721594682799L,
                           244267561200569511L, 244183445100924941L, 244261357185261158L,
                           244279703107679912L, 244228200211557414L, 244261337158153484L,
                           244227451447410631L, 244201543467010513L, 244227893247301128L,
                           244246479819133059L, 244252269008149096L, 244184301843340026L,
                           244262856161530351L, 244206721268567006L, 244222367078948618L,
                           244231701626781664L, 244281946628683598L};

        while (true) {
            try {
                Stopwatch stopwatch = Stopwatch.createStarted();
                Map<Long, byte[]> map = beast.multiGet(keys);
                System.out.println(stopwatch);
                stopwatch.reset().start();
                for (long key : keys)
                    beast.get(key);
                System.out.println(stopwatch);
                Thread.sleep(1000);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }
}
