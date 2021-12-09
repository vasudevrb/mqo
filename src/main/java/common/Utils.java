package common;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class Utils {

    private static final String AB = "BCEFGHIJKLMPQSTUVWXYZbcefghijklmpqstuvwxyz";
    private static final long QUERY_GEN_SEED = 152634546543L;
    private static final long seed = 14124987135L;

    private static final Random rnd = new Random(seed);
    private static final Random rnd2 = new Random();
    private static final Random queryGenRng = new Random(QUERY_GEN_SEED);

    public static String getPrintableSql(String sql) {
        return sql.replace(" FROM ", "\nFROM ")
                .replace(" WHERE ", "\nWHERE ")
                .replace(" GROUP BY ", "\nGROUP BY ");
    }

    public static String placeQuotes(String str) {
        return Arrays.stream(StringUtils.splitByWholeSeparator(str, ".")).map(x -> "\"" + x + "\"").collect(Collectors.joining("."));
    }

    public static String humanReadable(long bytes) {
        long absB = bytes == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(bytes);
        if (absB < 1024) {
            return bytes + " B";
        }
        long value = absB;
        CharacterIterator ci = new StringCharacterIterator("KMGTPE");
        for (int i = 40; i >= 0 && absB > 0xfffccccccccccccL >> i; i -= 10) {
            value >>= 10;
            ci.next();
        }
        value *= Long.signum(bytes);
        return String.format("%.1f %ciB", value / 1024.0, ci.current());
    }

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

    public static String randomString(int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++)
            sb.append(AB.charAt(rnd.nextInt(AB.length())));
        return sb.toString();
    }

    public static int getRandomGaussian() {
        return (int) (5 + rnd2.nextGaussian() * 3) * 1000;
    }

    public static int getRandomNumber(int limit) {
        return rnd2.nextInt(limit);
    }

    public static int getRandomNumber(int low, int high) {
        return rnd2.nextInt(low, high);
    }

    public static void main(String[] args) {
        System.out.println(isFloat("3.4134"));
    }

    public static double getQueryOperandBetween(List<Double> mnmx) {
        return queryGenRng.nextDouble(mnmx.get(0), mnmx.get(1));
    }

    public static <T> void shuffle(List<T> list) {
        Collections.shuffle(list, queryGenRng);
    }

    public static void restartPostgres() {
        String[] winCmdStart = {"C:\\Users\\Vasu\\Desktop\\Elevate.exe", "net", "start", "postgresql-x64-14"};
        String[] winCmdStop = {"C:\\Users\\Vasu\\Desktop\\Elevate.exe", "net", "stop", "postgresql-x64-14"};
        String[] linuxCmdStart = {"sudo", "-S", "usav1234", "service", "postgresql", "start"};
        String[] linuxCmdStop = {"sudo", "-S", "usav1234", "service", "postgresql", "stop"};

        try {
            runCommand(SystemUtils.IS_OS_WINDOWS_10 ? winCmdStop : linuxCmdStop);
            runCommand(SystemUtils.IS_OS_WINDOWS_10 ? winCmdStart : linuxCmdStart);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void runCommand(String[] command) {
        try {
            Process process = new ProcessBuilder(command).start();
            InputStream inputStream = process.getInputStream();
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                System.out.println(line);
            }
        } catch (Exception ex) {
            System.out.println("Exception : " + ex);
        }
    }
}
