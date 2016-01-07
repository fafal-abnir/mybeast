package amu.saeed.mybeast.spark;

import amu.saeed.mybeast.BeastConf;
import amu.saeed.mybeast.MyBeast;
import com.google.common.base.Stopwatch;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.sql.SQLException;
import java.util.ArrayList;

public class TestCompression {
    public static void main(String[] args) {
        String appName = "Test put";
        SparkConf conf = new SparkConf().setAppName(appName);
        SparkConfigurator.generalConfig(conf);


        Stopwatch stopwatch = Stopwatch.createStarted();
        JavaSparkContext sc = new JavaSparkContext(conf);
        conf.setMaster("spark://spark-master:7077");

        Accumulator<Integer> errCount = sc.accumulator(0);
        Accumulator<Integer> docCount = sc.accumulator(0);
        Accumulator<Integer> excCount = sc.accumulator(0);
        Accumulator<Long> rawSize = LongAccumolator.create();
        Accumulator<Long> gzSize = LongAccumolator.create();

        String input = args[0];
        int tasks = Integer.parseInt(args[1]);

        final BeastConf beastConf = new BeastConf();
        for (int i = 1; i <= 16; i++)
            beastConf.addMysqlShard(String.format("jdbc:mysql://mysql-%d/kv%d", i, i)
                    + "?useUnicode=true&useConfigs=maxPerformance"
                    + "&characterEncoding=UTF-8&user=root&password=chamran");


        JavaPairRDD<LongWritable, Text> inputRecords =
                sc.sequenceFile(input, LongWritable.class, Text.class, tasks);
        inputRecords = inputRecords.mapValues(t -> new Text()).repartition(tasks);
        inputRecords.mapPartitions(part -> {
            final MyBeast beast = new MyBeast(beastConf);
            part.forEachRemaining(t -> {
                docCount.add(1);
                //                String json = t._2().toString();
                //                rawSize.add((long) json.getBytes().length);
                //                byte[] compressed = TextUtils.compressAndFit64k(json);
                //                rawSize.add((long) json.getBytes().length);
                //                gzSize.add((long) compressed.length);
                try {
                    //                    for (int i = 0; i < 1; i++)
                    //                        beast.put(t._1.get() + i, compressed);
                    if (beast.get(t._1.get()) == null)
                        errCount.add(1);

                } catch (SQLException e) {
                    excCount.add(1);
                }

            });
            return new ArrayList();
        }).count();


        System.out.printf("Docs: %,d \n", docCount.value());
        System.out.printf("ERR: %,d \n", errCount.value());
        System.out.printf("EXC: %,d \n", excCount.value());
        System.out.printf("RAW: %,d \n", rawSize.value());
        System.out.printf("GZIP: %,d \n", gzSize.value());
        System.out.println("Time: " + stopwatch);

    }
}
