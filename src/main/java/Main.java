import cache.Cache;
import common.Configuration;

import java.io.PrintStream;
import java.util.Arrays;

import static com.diogonunes.jcolor.Ansi.colorize;
import static com.diogonunes.jcolor.Attribute.MAGENTA_BACK;
import static com.diogonunes.jcolor.Attribute.YELLOW_TEXT;

public class Main {

    private static final String TEST_MODE_CACHE_SIZE = "testCacheSize";

    public static void main(String[] args) throws Exception {
        hideLoggerWarnings();

        Configuration config = Configuration.initialize();

        if (args[0].equals(TEST_MODE_CACHE_SIZE)) {
            Tester tester = new Tester(config);
            tester.testCacheSizeMetrics(Cache.SIZES_MB.get(Integer.parseInt(args[1])));
        }

        System.out.println(colorize("Got arguments " + Arrays.toString(args), YELLOW_TEXT(), MAGENTA_BACK()));
    }

    public static void hideLoggerWarnings() {
        PrintStream filterOut = new PrintStream(System.err) {
            public void println(String l) {
                if (!l.startsWith("SLF4J")) {
                    super.println(l);
                }
            }
        };
        System.setErr(filterOut);
    }

}
