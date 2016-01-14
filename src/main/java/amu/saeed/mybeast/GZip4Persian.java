package amu.saeed.mybeast;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import org.tukaani.xz.LZMA2Options;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GZip4Persian {
    static final private Charset UTF8 = Charset.forName("UTF-8");
    static HashMap<Character, Character> map = new HashMap<>();
    private static LZMA2Options lzma2Options = new LZMA2Options();

    // It is nasty and inefficient but let it live just for now.
    static {
        map.put('ا', (char) 15);
        map.put('ی', (char) 16);
        map.put('ر', (char) 17);
        map.put('ن', (char) 18);
        map.put('و', (char) 19);
        map.put('د', (char) 20);
        map.put('م', (char) 21);
        map.put('ه', (char) 22);
        map.put('ت', (char) 23);
        map.put('ب', (char) 24);
        map.put('س', (char) 25);
        map.put('ل', (char) 26);
        map.put('ش', (char) 27);
        map.put('ک', (char) 28);
        map.put('ز', (char) 29);
        map.put('ف', (char) 30);
        map.put('ع', (char) 31);

        map.put((char) 15, 'ا');
        map.put((char) 16, 'ی');
        map.put((char) 17, 'ر');
        map.put((char) 18, 'ن');
        map.put((char) 19, 'و');
        map.put((char) 20, 'د');
        map.put((char) 21, 'م');
        map.put((char) 22, 'ه');
        map.put((char) 23, 'ت');
        map.put((char) 24, 'ب');
        map.put((char) 25, 'س');
        map.put((char) 26, 'ل');
        map.put((char) 27, 'ش');
        map.put((char) 28, 'ک');
        map.put((char) 29, 'ز');
        map.put((char) 30, 'ف');
        map.put((char) 31, 'ع');
    }

    private static String transform(String s) {
        Preconditions.checkNotNull(s);
        StringBuilder translated = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            Character ch = map.get(s.charAt(i));
            if (ch != null)
                translated.append(ch);
            else
                translated.append(s.charAt(i));
        }
        return translated.toString();
    }

    private static String deTransform(String s) {
        Preconditions.checkNotNull(s);
        StringBuilder translated = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            Character ch = map.get(s.charAt(i));
            if (ch != null)
                translated.append(ch);
            else
                translated.append(s.charAt(i));
        }
        return translated.toString();
    }

    public static byte[] compress(String s) {
        Preconditions.checkNotNull(s);
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(s.length());
            GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
            gzipOutputStream.write(transform(s).getBytes(UTF8));
            gzipOutputStream.close();
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static String uncompress(byte[] bytes) {
        Preconditions.checkNotNull(bytes);
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(bytes.length * 4);
            GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(bytes));
            ByteStreams.copy(gzipInputStream, byteArrayOutputStream);
            return deTransform(new String(byteArrayOutputStream.toByteArray(), UTF8));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static byte[] compressAndFit(String s, int maxCompressedSize) {
        byte[] compressed = null;
        int step = Integer.max(s.length() / 10, 1024);
        do {
            compressed = GZip4Persian.compress(s);
            if (compressed.length > maxCompressedSize)
                s = s.substring(0, s.length() - step);
        } while (compressed.length > maxCompressedSize);
        Preconditions.checkState(compressed.length <= maxCompressedSize,
                "This must never happen! The BASTARD was not reduced to lower than 64ks.");
        return compressed;
    }

}
