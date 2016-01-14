package amu.saeed.mybeast.spark;

import amu.saeed.mybeast.BeastConf;
import amu.saeed.mybeast.GZip4Persian;
import amu.saeed.mybeast.MyBeast;
import com.google.common.base.Stopwatch;
import com.google.common.hash.Hashing;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import scala.Tuple2;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;

public class InsertionTest {
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
        //conf.setMaster("spark://spark-master:7077");

        Accumulator<Integer> rowCount = sc.accumulator(0);
        Accumulator<Integer> cutCount = sc.accumulator(0);
        Accumulator<Integer> excCount = sc.accumulator(0);
        Accumulator<Long> rawSize = LongAccumolator.create();
        Accumulator<Long> gzSize = LongAccumolator.create();
        Accumulator<Long> rows1k = LongAccumolator.create();
        Accumulator<Long> rows2k = LongAccumolator.create();
        Accumulator<Long> rows3k = LongAccumolator.create();
        Accumulator<Long> rows4k = LongAccumolator.create();
        Accumulator<Long> rows6k = LongAccumolator.create();
        Accumulator<Long> rows8k = LongAccumolator.create();


        final BeastConf beastConf = new BeastConf();
        for (int i = 1; i <= 16; i++)
            beastConf.addMysqlShard(String.format("jdbc:mysql://mysql-%d/kv%d", i, i)
                    + "?useUnicode=true&useConfigs=maxPerformance"
                    + "&characterEncoding=UTF-8&user=root&password=chamran");


        JavaPairRDD<LongWritable, Text> inputRecords =
                sc.sequenceFile(params.inputPath, LongWritable.class, Text.class, params.numTasks);
        JavaPairRDD<LongWritable, Text> hashes = inputRecords.mapPartitionsToPair(part -> {
            ArrayList<Tuple2<LongWritable, Text>> hashList = new ArrayList<>();
            final MyBeast beast = new MyBeast(beastConf);
            part.forEachRemaining(t -> {
                try {
                    rowCount.add(1);
                    byte[] compressed = GZip4Persian.compress(t._2().toString());
                    if (compressed.length > MyBeast.MAX_BEAST_BLOB_SIZE) {
                        compressed =
                                GZip4Persian.compressAndFit(t._2().toString(), MyBeast.MAX_BEAST_BLOB_SIZE);
                        cutCount.add(1);
                    }
                    rawSize.add((long) t._2.getBytes().length);
                    gzSize.add((long) compressed.length);
                    if (compressed.length < 1024 * 1)
                        rows1k.add(1L);
                    else if (compressed.length < 1024 * 2)
                        rows2k.add(1L);
                    else if (compressed.length < 1024 * 3)
                        rows3k.add(1L);
                    else if (compressed.length < 1024 * 4)
                        rows4k.add(1L);
                    else if (compressed.length < 1024 * 6)
                        rows6k.add(1L);
                    else if (compressed.length < 1024 * 8)
                        rows8k.add(1L);

                    beast.put(t._1.get(), compressed);
                    hashList.add(new Tuple2<>(t._1(), new Text(
                            Hashing.sha1().hashString(t._2().toString(), Charset.forName("UTF8"))
                                    .toString())));
                } catch (SQLException e) {
                    excCount.add(1);
                }
            });
            return hashList;
        });

        hashes.saveAsHadoopFile(params.outputPath, LongWritable.class, Text.class,
                SequenceFileOutputFormat.class, GzipCodec.class);

        System.out.printf("rows: %,d\n", rowCount.value());
        System.out.printf("exp: %,d\n", excCount.value());
        System.out.printf("cut: %,d\n", cutCount.value());
        System.out.printf("raw: %,d\n", rawSize.value());
        System.out.printf("gzip: %,d\n", gzSize.value());
        System.out.printf("1k: %,d\n", rows1k.value());
        System.out.printf("2k: %,d\n", rows2k.value());
        System.out.printf("3k: %,d\n", rows3k.value());
        System.out.printf("4k: %,d\n", rows4k.value());
        System.out.printf("6k: %,d\n", rows6k.value());
        System.out.printf("8k: %,d\n", rows8k.value());
        System.out.println("Time: " + stopwatch);

    }


    private static class Params implements Serializable {
        @Option(name = "-t", usage = "number of tasks to launch", required = true)
        int numTasks;
        @Option(name = "-i", usage = "the input path", required = true)
        String inputPath;
        @Option(name = "-o", usage = "the output path", required = false)
        String outputPath = null;
    }
}
