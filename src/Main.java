import common.Configuration;

import java.io.PrintStream;

public class Main {


    public static void main(String[] args) throws Exception {
        hideLoggerWarnings();

        Configuration config = Configuration.initialize();
        Window window = new Window(config);
        window.run();
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
