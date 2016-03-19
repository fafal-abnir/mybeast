package amu.saeed.mybeast.spark;

import org.apache.spark.SparkConf;

/**
 * Created by Saeed on 7/18/2015.
 */
public class SparkConfigurator {

    private static void commonConfig(SparkConf conf) {
        conf.set("spark.kryoserializer.buffer.max", "512m");
        conf.set("spark.akka.frameSize", "128");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.rdd.compress", "true");
        conf.set("spark.io.compression.codec", "lz4");
        conf.set("spark.shuffle.consolidateFiles", "true");
        conf.set("spark.akka.threads", "4");
        conf.set("spark.hadoop.mapred.output.compress", "true");
        conf.set("spark.hadoop.mapred.output.compression.codec",
                 "org.apache.hadoop.io.compress.DefaultCodec");
        conf.set("spark.hadoop.mapred.output.compression.type", "BLOCK");

    }

    public static void generalConfig(SparkConf conf) {
        commonConfig(conf);
        conf.set("spark.executor.memory", "2g");
        conf.set("spark.storage.memoryFraction", "0.25");
    }
}
