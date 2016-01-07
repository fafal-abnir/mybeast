package amu.saeed.mybeast;

import com.google.common.base.Preconditions;

public class TextUtils {
    public static byte[] compressAndFit64k(String s) {
        byte[] compressed = null;
        int step = Integer.max(s.length() / 10, 1024);
        do {
            compressed = GZip4Persian.compress(s);
            if (compressed.length > 65535)
                s = s.substring(0, s.length() - step);
        } while (compressed.length > 65535);
        Preconditions.checkState(compressed.length <= 65535,
                "This must never happen! The BASTARD was not reduced to lower than 64ks.");
        return compressed;
    }
}
