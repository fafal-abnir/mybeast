package amu.saeed.mybeast.spark;

import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;

/**
 * Created by saeed on 6/10/15.
 */
public class LongAccumolator implements AccumulatorParam<Long> {
    public static Accumulator<Long> create() {
        return new Accumulator<>(0L, new LongAccumolator());
    }

    @Override
    public Long addAccumulator(Long aLong, Long aLong2) {
        return aLong + aLong2;
    }

    @Override
    public Long addInPlace(Long aLong, Long r1) {
        return aLong + r1;
    }

    @Override
    public Long zero(Long aLong) {
        return 0L;
    }
}
