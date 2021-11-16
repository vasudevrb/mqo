import batch.QueryBatcher;
import batch.data.BatchedQuery;
import common.Configuration;
import common.QueryExecutor;
import common.QueryUtils;
import common.Utils;
import mv.MViewOptimizer;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import test.QueryProvider;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static common.Utils.randomString;

public class Window {

    private final QueryExecutor executor;
    private final QueryProvider provider;

    private final QueryBatcher batcher;
    private final MViewOptimizer optimizer;

    public Window(Configuration configuration) {
        this.executor = new QueryExecutor(configuration);
        this.provider = new QueryProvider();

        this.batcher = new QueryBatcher(configuration, executor);
        this.optimizer = new MViewOptimizer(configuration);
    }

    public void run() throws SQLException {
//        List<String> queries = provider.getAllBatches();
        String mv = """
                SELECT "l_shipdate", COUNT(*) as "cd" \
                FROM "public"."lineitem" \
                WHERE "l_shipdate" >= date '1994-03-01' \
                AND "l_shipdate" < date '1994-06-01' \
                AND "l_discount" between 0.06 AND 0.07 \
                AND "l_quantity" < 18
                GROUP BY "l_shipdate"
                """;

        String q = """
                SELECT "l_shipdate", COUNT(*) as "cncn" \
                FROM "public"."lineitem" \
                WHERE "l_shipdate" >= date '1994-04-01' \
                AND "l_shipdate" < date '1994-05-01' \
                AND "l_discount" between 0.06 AND 0.07 \
                AND "l_quantity" < 18
                GROUP BY "l_shipdate"
                """;

        List<String> queries = new ArrayList<>();
        queries.addAll(Arrays.asList(mv, q));
        Collections.shuffle(queries);

        long t1 = System.currentTimeMillis();
        for (String s : queries) {
            executor.execute(executor.getLogicalPlan(s), null);
        }
        System.out.println("IND: " + (System.currentTimeMillis() - t1) + " ms");

        List<BatchedQuery> batchedQueries = batcher.batch(queries);

        for (BatchedQuery bq : batchedQueries) {
            System.out.println();
            System.out.println(Utils.getPrintableSql(bq.sql));
            System.out.println();

            RelOptMaterialization materialization = optimizer.materialize(randomString(4), bq.sql);
            for (SqlNode node : bq.parts.subList(1, bq.parts.size())) {
                RelNode relNode = optimizer.substitute(materialization, executor.getLogicalPlan(node));
                if (relNode != null) {
                    System.out.println(relNode.explain());
                    executor.execute(relNode, rs -> System.out.println(QueryUtils.countRows(rs)));
                }
            }
        }
    }
}
