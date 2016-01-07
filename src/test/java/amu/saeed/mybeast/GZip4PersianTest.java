package amu.saeed.mybeast;

import org.junit.Assert;
import org.junit.Test;

public class GZip4PersianTest {

    private static final String s2 = "این واژه به خودی خود معنی مقدسی ندارد و در قرآن "
            + "هم برای پیشوایانی که مردم را به سوی خدا هدایت می\u200Cکردند به کار رفته و هم برای "
            + "پیشوایانی که مردم را به سوی کفر هدایت می\u200Cکرده\u200Cاند.";
    private static final String s1 = "The Apache Commons Compress library defines an API for working with"
            + "cpio, Unix dump, tar, zip, gzip, XZ, Pack200, bzip2, 7z, lzma, snappy, DEFLATE and Z files.";


    @Test
    public void testCompress() throws Exception {
        Assert.assertEquals(GZip4Persian.uncompress(GZip4Persian.compress(s1)), s1);
        Assert.assertEquals(GZip4Persian.uncompress(GZip4Persian.compress(s2)), s2);
    }
}
