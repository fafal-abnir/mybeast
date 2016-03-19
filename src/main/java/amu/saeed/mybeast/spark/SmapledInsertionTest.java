package amu.saeed.mybeast.spark;

import amu.saeed.mybeast.BeastConf;
import amu.saeed.mybeast.GZip4Persian;
import amu.saeed.mybeast.MyBeast;
import amu.saeed.mybeast.MysqlStore;
import com.google.common.base.Stopwatch;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SmapledInsertionTest {
    public static void main(String[] args) {
        final Params params = new Params();
        try {
            new CmdLineParser(params).parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            new CmdLineParser(params).printUsage(System.err);
            return;
        }

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

        final BeastConf beastConf = new BeastConf();
        for (int i = 1; i <= 16; i++)
            beastConf.addMysqlShard(String.format("jdbc:mysql://mysql-%d/kv%d", i, i) +
                                            "?useUnicode=true&useConfigs=maxPerformance" +
                                            "&characterEncoding=UTF-8&user=root"
                                            + "&password=chamran");

        JavaPairRDD<LongWritable, Text> inputRecords = sc.sequenceFile(params.inputPath,
                                                                       LongWritable.class,
                                                                       Text.class, params.numTasks);
        JavaPairRDD<Long, Integer> sizes = inputRecords.flatMapToPair(t -> {
            List<Tuple2<Long, Integer>> list = new ArrayList<>();
            int len = GZip4Persian.compressAndFit(t._2().toString(),
                                                  MysqlStore.MAX_VALUE_LEN).length;
            for (int i = 0; i < params.repFactor; i++)
                list.add(new Tuple2<>(t._1.get() + i, len));
            return list;
        });

        sizes = sizes.repartition(params.numTasks).cache();

        sizes.map(t -> String.format("%d\t%d", t._1, t._2)).saveAsTextFile(params.outputPath,
                                                                           GzipCodec.class);

        sizes.mapPartitions(part -> {
            final MyBeast beast = new MyBeast(beastConf);
            part.forEachRemaining(t -> {
                try {
                    beast.put(t._1, new byte[t._2]);
                } catch (SQLException e) {
                    excCount.add(1);
                }
            });
            return new ArrayList<>();
        }).count();

        //        sizes.repartition(tasks).mapPartitions(part -> {
        //            final MyBeast beast = new MyBeast(beastConf);
        //            for (int i = 0; i < 2_000_000_000 / sampleSize; i++) {
        //                final int k = i;
        //                part.forEachRemaining(t -> {
        //                    try {
        //                        beast.put(t._1 + k, new byte[t._2]);
        //                    } catch (SQLException e) {
        //                        excCount.add(1);
        //                    }
        //                });
        //            }
        //            return new ArrayList<>();
        //        }).count();

        System.out.printf("Rows: %,d\n", sizes.count());
        System.out.printf("Exc: %,d\n", excCount.value());
        System.out.println("Time: " + stopwatch);

    }

    private static class Params implements Serializable {
        @Option(name = "-f", usage = "multiply factor", required = true)
        int repFactor;
        @Option(name = "-t", usage = "number of tasks to launch", required = true)
        int numTasks;
        @Option(name = "-i", usage = "the input path", required = true)
        String inputPath;
        @Option(name = "-o", usage = "the output path", required = false)
        String outputPath = null;
    }
}
