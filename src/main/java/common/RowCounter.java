package common;

import java.sql.ResultSet;
import java.util.concurrent.atomic.AtomicLong;

public class RowCounter {

    public static long countRows(String query, QueryExecutor executor) {
        AtomicLong rowCount = new AtomicLong();
        String cover = String.format("SELECT count(*) FROM (%s) as tab", query);
        executor.execute(executor.validate(cover), rs -> {
            rowCount.set(_count(rs));
        });

        return rowCount.get();
    }

    private static int _count(ResultSet rs) {
        try {
            rs.next();
            return rs.getInt(1);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return 0;
    }
}
