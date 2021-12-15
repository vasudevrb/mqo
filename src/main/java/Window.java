import batch.QueryBatcher;
import batch.data.BatchedQuery;
import cache.Cache;
import cache.CacheItem;
import cache.dim.Dimension;
import cache.policy.ReplacementPolicy;
import common.*;
import mv.MViewOptimizer;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.io.FileUtils;
import test.QueryProvider;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static common.Logger.*;
import static common.Utils.humanReadable;

public class Window {

    private final QueryExecutor executor;
    private final QueryProvider provider;

    private final QueryBatcher batcher;
    private final MViewOptimizer optimizer;
    private final Cache<RelOptMaterialization> cache;
    //This tracks the time taken to calculate the size of the materialized view
    //so that we can subtract this from the execution time.
    public long subtractable;

    public Window(Configuration configuration, int sizeMB, ReplacementPolicy<RelOptMaterialization> policy) {
        this.executor = new QueryExecutor(configuration);
        this.provider = new QueryProvider();

        this.batcher = new QueryBatcher(configuration, executor);
        this.optimizer = new MViewOptimizer(configuration);

        this.cache = new Cache<>(policy, Dimension.SIZE(sizeMB * FileUtils.ONE_MB));
    }

    public void run() {
        //[0] is the cardinal count, [1] is the number of queries
        int[] count = new int[2];

        final long t1 = System.currentTimeMillis();
        provider.listen(qs -> {
            System.out.println("===============================================");
            System.out.printf("%d: (%d)\nNo. of queries: %d\n", count[0], count[1], qs.size());

            if (count[0] % 32 == 0) {
                long time = System.currentTimeMillis() - t1 - subtractable - CustomPlanner.diff;
                System.out.println();
                logFinalTime("Completed executing " + count[0] + " queries in " + time + "ms");
                System.out.println();
            }

            count[0] += 1;
            count[1] += qs.size();

//            runSequentially(qs);
            handle(qs);
            if (count[1] > 640) {
                long time = System.currentTimeMillis() - t1 - subtractable - CustomPlanner.diff;
                System.out.println();
                logTime("Stopping... Time: " + time + " ms");
                provider.stopListening();
            }
        });
    }

    private void runSequentially(List<String> queries) {
        for (String query : queries) {
            executor.execute(executor.getLogicalPlan(query), null);
        }
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
        for (CacheItem<RelOptMaterialization> item : cache.getItems()) {
            RelOptMaterialization materialization = item.getItem();
            substituted = optimizer.substitute(materialization, logicalPlan);
            if (substituted != null) {
                return substituted;
            }
        }
        return null;
    }

    private RelNode getSubstitution(RelOptMaterialization materialization, RelNode logicalPlan) {
        return optimizer.substitute(materialization, logicalPlan);
    }

    private void runIndividualQuery(String q) {
        RelNode logicalPlan = executor.getLogicalPlan(q);
        RelNode substituted = getSubstitution(logicalPlan);

        if (substituted == null) {
            RelOptMaterialization materialization = optimizer.materialize(q, logicalPlan);

            long t1 = System.currentTimeMillis();
            long value = cache.getDimension().getType() == Dimension.Type.SIZE_BYTES
                    ? QueryUtils.getTableSize(q, materialization, executor)
                    : 1;
            subtractable += (System.currentTimeMillis() - t1);
            logTime("Calculating table size took " + (System.currentTimeMillis() - t1) + " ms, Size:" + humanReadable(value));

            cache.add(materialization, value);
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
            System.out.println();
            System.out.println("Batched SQL: " + Utils.getPrintableSql(bq.sql));
            System.out.println();
            RelNode plan = executor.getLogicalPlan(bq.sql);
            RelNode substitutable = getSubstitution(plan);
            RelOptMaterialization materialization = null;
            if (substitutable == null) {
                materialization = optimizer.materialize(bq.sql, plan);

                long t1 = System.currentTimeMillis();
                long value = cache.getDimension().getType() == Dimension.Type.SIZE_BYTES
                        ? QueryUtils.getTableSize(bq.sql, materialization, executor)
                        : 1;
                subtractable += (System.currentTimeMillis() - t1);
                logTime("Calculating table size took " + (System.currentTimeMillis() - t1) + " ms, Size:" + humanReadable(value));

                cache.add(materialization, value);
            }

            for (SqlNode partQuery : bq.parts) {
                RelNode partSubstitutable = materialization != null
                        ? getSubstitution(materialization, executor.getLogicalPlan(partQuery))
                        : getSubstitution(executor.getLogicalPlan(partQuery));

                if (partSubstitutable == null) {
                    logError("This shouldn't happen!!!!!! Batch query is substitutable but parts are not");
                    System.out.println(partQuery.toString());
                    System.out.println();
                    return;
                }
                executor.execute(partSubstitutable, rs -> System.out.println("MVS Part Executed " + bq.sql));
                System.out.println();
            }
        }
    }
}
