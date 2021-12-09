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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static common.Logger.logTime;

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

//        this.cache = new Cache<>(new LRUPolicy<>(), Dimension.COUNT(30));
        this.cache = new Cache<>(policy, Dimension.SIZE(sizeMB * FileUtils.ONE_MB));
    }

    public void run() {
        AtomicInteger count = new AtomicInteger();
        final long t1 = System.currentTimeMillis();
        provider.listen(qs -> {
            System.out.println("===============================================");
            System.out.println(count.get() + "\n" + Utils.getPrintableSql(qs.get(0)));
            count.getAndIncrement();
//            runSequentially(qs);
            handle(qs);
            if (count.get() == 32) {
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
            logTime("Calculating table size took " + (System.currentTimeMillis() - t1) + " ms, Size:" + FileUtils.byteCountToDisplaySize(value));

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
            System.out.println("Batched SQL: " + Utils.getPrintableSql(bq.sql));
            RelNode plan = executor.getLogicalPlan(bq.sql);
            RelNode substitutable = getSubstitution(plan);
            if (substitutable == null) {
                RelOptMaterialization materialization = optimizer.materialize(bq.sql, plan);

                long t1 = System.currentTimeMillis();
                long value = cache.getDimension().getType() == Dimension.Type.SIZE_BYTES
                        ? QueryUtils.getTableSize(bq.sql, materialization, executor)
                        : 1;
                subtractable += (System.currentTimeMillis() - t1);

                cache.add(materialization, value);
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
