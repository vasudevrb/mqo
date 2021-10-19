package util;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class ExpressionCalculator {

    public static List<Boolean> run(Function<Integer[], Boolean> func, int min, int max) {
        List<Boolean> result = new ArrayList<>();
        for (int i = min; i < max; i++) {
            for (int j = min; j < max; j++) {
                for (int k = min; k < max; k++) {
                    result.add(func.apply(new Integer[]{i, j, k}));
                }
            }
        }
        return result;
    }
}
