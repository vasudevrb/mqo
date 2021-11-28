import batch.QueryBatcher;
import batch.data.BatchedQuery;
import common.Configuration;
import common.QueryExecutor;
import common.Utils;
import mv.MViewOptimizer;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import test.QueryProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Window {

    private final QueryExecutor executor;
    private final QueryProvider provider;

    private final QueryBatcher batcher;
    private final MViewOptimizer optimizer;

    private final ArrayList<RelOptMaterialization> materializations;

    public Window(Configuration configuration) {
        this.executor = new QueryExecutor(configuration);
        this.provider = new QueryProvider();

        this.batcher = new QueryBatcher(configuration, executor);
        this.optimizer = new MViewOptimizer(configuration);

        this.materializations = new ArrayList<>();
    }

    public void testBatch() {
        String q1 = """
                SELECT count(*), "l_quantity", "l_discount" \
                FROM "lineitem" \
                WHERE "l_discount" < 0.08 \
                AND "l_quantity" < 25 \
                GROUP BY "l_quantity", "l_discount"
                """;

        String q2 = """
                SELECT count("l_quantity"), "l_quantity" \
                FROM "lineitem" \
                WHERE "l_discount" < 0.04 \
                AND "l_quantity" < 30 \
                GROUP BY "l_quantity"
                """;

        List<BatchedQuery> b = batcher.batch(List.of(q2, q1));
        System.out.println(Utils.getPrintableSql(b.get(0).sql));

        RelOptMaterialization m = optimizer.materialize("asda", b.get(0).sql);

        if (optimizer.substitute(m, executor.getLogicalPlan(q1)) != null) {
            System.out.println("CAN SUB 1");
        }

        if (optimizer.substitute(m, executor.getLogicalPlan(q2)) != null) {
            System.out.println("CAN SUB 2");
        }
//        executor.execute(executor.validate(b.get(0).sql), rs -> System.out.println("Rows: " + QueryUtils.countRows(rs)));
    }

    public void run() {
        AtomicInteger count = new AtomicInteger();
        provider.listen(qs -> {
            count.getAndIncrement();
            System.out.println("===============================================");
            handle(qs);
            if (count.get() == 5) {
                System.out.println("Stopping...");
                provider.stopListening();
            }
        });
    }

    private void handle(List<String> queries) {
        if (queries.size() == 1) {
            runIndividualQuery(queries.get(0));
        } else {
            runBatchQueries(queries);
        }
    }

    private RelNode getSubstitution(RelNode logicalPlan) {
        RelNode substituted;
        for (RelOptMaterialization materialization : materializations) {
            substituted = optimizer.substitute(materialization, logicalPlan);
            if (substituted != null) {
                return substituted;
            }
        }
        return null;
    }

    private void runIndividualQuery(String q) {
        RelNode logicalPlan = executor.getLogicalPlan(q);
        RelNode substituted = getSubstitution(logicalPlan);

        if (substituted == null) {
            RelOptMaterialization materialization = optimizer.materialize(Utils.randomString(4), q);
            materializations.add(materialization);
            //TODO: Profile this, is this executed again? If so, find a way to extract results from
            //TODO: materialized table
            executor.execute(logicalPlan, rs -> System.out.println("Executed " + q.replace("\n", " ")));
        } else {
            executor.execute(substituted, rs -> System.out.println("MVS Executed " + q.replace("\n", " ")));
        }
    }

    private void runBatchQueries(List<String> queries) {
        List<BatchedQuery> batched = batcher.batch(queries);

        // Find out all the queries from the list that couldn't be batched and run them individually
        List<Integer> batchedIndexes = batched.stream().flatMap(bq -> bq.indexes.stream()).collect(Collectors.toList());
        List<Integer> unbatchedIndexes = IntStream.range(0, queries.size()).boxed().collect(Collectors.toList());
        unbatchedIndexes.removeAll(batchedIndexes);
        for (int i : unbatchedIndexes) {
            runIndividualQuery(queries.get(i));
        }

        // Execute batched queries
        // For each batched query find out if any materialized view can be used
        // If not, then execute the batch queries individually
        // If yes, then it means that the batch query parts can also use that same MV
        // Find substitutions and execute
        for (BatchedQuery bq : batched) {
            System.out.println("Batched SQL: " + Utils.getPrintableSql(bq.sql));
            RelNode substitutable = getSubstitution(executor.getLogicalPlan(bq.sql));
            if (substitutable == null) {
                RelOptMaterialization materialization = optimizer.materialize(Utils.randomString(4), bq.sql);
                materializations.add(materialization);
            }

            for (SqlNode partQuery : bq.parts) {
                RelNode partSubstitutable = getSubstitution(executor.getLogicalPlan(partQuery));
                if (partSubstitutable == null) {
                    System.out.println("This shouldn't happen!!!!!! Batch query is substitutable but parts are not");
                    return;
                }
                executor.execute(partSubstitutable, rs -> System.out.println("MVS Part Executed " + bq.sql));
            }
        }
    }
}
