import common.Configuration;
import common.QueryExecutor;
import test.QueryReader;

import java.io.PrintStream;
import java.util.List;

public class Main {


    public static void main(String[] args) throws Exception {
        hideLoggerWarnings();

        Configuration config = Configuration.initialize();
//        Window window = new Window(config);
//        window.run();

//        Tester tester = new Tester(config);
//        tester.testBatch();

        List<String> queries = QueryReader.getQueries(10);

        QueryExecutor executor = new QueryExecutor(config);
        for (int i = 0; i < queries.size(); i++) {
            System.out.println("Validating..." + i);
            executor.validate(queries.get(i));
        }
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
