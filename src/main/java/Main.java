import cache.Cache;
import cache.policy.FIFOPolicy;
import cache.policy.LFUPolicy;
import cache.policy.LRUPolicy;
import cache.policy.ReplacementPolicy;
import common.Configuration;
import org.apache.calcite.plan.RelOptMaterialization;

import java.io.PrintStream;

public class Main {

    private static final String TEST_MODE_CACHE_SIZE = "testCacheSize";

    public static void main(String[] args) throws Exception {
        hideLoggerWarnings();

//        args = new String[]{"testCacheSize", "5", "lru"};

        Configuration config = Configuration.initialize();

        Tester tester = new Tester(config);
        if (args.length > 0 && args[0].equals(TEST_MODE_CACHE_SIZE)) {
            int size = Cache.SIZES_MB.get(Integer.parseInt(args[1]));
            ReplacementPolicy<RelOptMaterialization> pol = args[2].equals("fifo") ? new FIFOPolicy<>()
                    : args[2].equals("lfu") ? new LFUPolicy<>()
                    : new LRUPolicy<>();

            tester.testCacheSizeMetrics(size, pol);
        }

//        tester.testFindDerivablePercentage();
        tester.printQuerySizes();
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
