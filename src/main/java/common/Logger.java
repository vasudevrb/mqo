package common;

import static com.diogonunes.jcolor.Ansi.colorize;
import static com.diogonunes.jcolor.Attribute.*;

public class Logger {

    public static void logCache(String log) {
        System.out.println(colorize(log, YELLOW_TEXT(), MAGENTA_BACK()));
    }

    public static void logTime(String log) {
        System.out.println(colorize(log, RED_TEXT(), WHITE_BACK()));
    }

    public static void logFinalTime(String log) {
        System.out.println(colorize(log, WHITE_TEXT(), BLUE_BACK()));
    }
}
