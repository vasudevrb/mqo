import common.Configuration;
import test.QueryReader;

import java.io.PrintStream;
import java.util.List;

public class Main {

    public static final List<Integer> CACHE_SIZES = List.of(4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096);
    public static final List<String> DERIVABILITIES = List.of("43", "53", "55", "57", "58", "61", "63", "66",
            "70", "75", "80", "83", "87", "90", "93", "97");


    private static final String TEST_MODE_CACHE_SIZE = "testCacheSize";

    /*
    The execution is managed by command line arguments
        [mode] [cache size] [derivability]

        mode: 0 for sequential test, 1 for hybrid test
        cache size: [0 10] == [4MB 4GB]
        derivability: [0 15]
     */

    public static void main(String[] args) throws Exception {
        hideLoggerWarnings();

        System.out.println("########################################################################################");
        System.out.println("########################################################################################");
        System.out.println("########################################################################################");

        int modeArg = Integer.parseInt(args[0]);
        int cacheSizeArg = Integer.parseInt(args[1]);
        int derivabilityArg = Integer.parseInt(args[2]);

        Configuration config = Configuration.initialize();
        Tester tester = new Tester(config);

        String mode = modeArg == 0 ? "SEQ" : "HYB";
        int size = CACHE_SIZES.get(cacheSizeArg);
        String der = DERIVABILITIES.get(derivabilityArg);

        System.out.printf("Starting with mode: %s, cache size: %dMB, derivability: %s\n", mode, size, DERIVABILITIES.get(derivabilityArg));

        QueryReader.dir = der;
        tester.testMain(modeArg == 0, size);

//        tester.testFindDerivablePercentage();
//        tester.testMVSubstitution();
//        tester.testDerivabilityPerf();
//        tester.printQuerySizes();
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
