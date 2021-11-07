package common;

import org.apache.commons.lang.StringUtils;

import java.util.Random;

public class Utils {

    public static boolean isInt(String val) {
        return StringUtils.isNumeric(val);
    }

    public static boolean isFloat(String str) {
        if (str == null) {
            return false;
        }
        int length = str.length();
        if (length == 0) {
            return false;
        }
        int i = 0;
        if (str.charAt(0) == '-') {
            if (length == 1) {
                return false;
            }
            i = 1;
        }
        for (; i < length; i++) {
            char c = str.charAt(i);
            if ((c < '0' || c > '9') && c != '.') {
                return false;
            }
        }
        return true;
    }

    public static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    static final String AB = "BCEFGHIJKLMPQSTUVWXYZbcefghijklmpqstuvwxyz";
    static long seed = 14124987135L;
    static Random rnd = new Random(seed);

    public static String randomString(int len){
        StringBuilder sb = new StringBuilder(len);
        for(int i = 0; i < len; i++)
            sb.append(AB.charAt(rnd.nextInt(AB.length())));
        return sb.toString();
    }

    public static int getRandomGaussian() {
        return (int) (5 + rnd.nextGaussian() * 3) * 1000;
    }

    public static int getRandomNumber(int limit) {
        return rnd.nextInt(limit);
    }

    public static void main(String[] args) {
        System.out.println(isFloat("3.4134"));
    }
}
