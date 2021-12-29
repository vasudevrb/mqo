import batch.QueryBatcher;
import batch.data.BatchedQuery;
import cache.Cache;
import cache.dim.Dimension;
import cache.policy.ReplacementPolicy;
import common.*;
import mv.MViewOptimizer;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import test.QueryProvider;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static common.Logger.logError;
import static common.Logger.logTime;
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

    public void run(boolean runSequentially) {
        int count = 0, numQueries = 0;

        final long t1 = System.currentTimeMillis();
        for (List<String> qs : provider.queries) {
            System.out.println("===============================================");
            System.out.printf("%d: (%d)\nNo. of queries: %d\n", count, numQueries, qs.size());

            if (count % 5 == 0) {
                long time = System.currentTimeMillis() - t1 - subtractable - CustomPlanner.diff;
                logTime(String.format("Executed: %d: (%d) in %d ms", count, numQueries, time));
            }

            count += 1;
            numQueries += qs.size();

            if (runSequentially) runSequentially(qs);
            else handle(qs);
        }

        long time = System.currentTimeMillis() - t1 - subtractable;
        logTime("Stopping... Time: " + time + " ms, Time no sub: " + (System.currentTimeMillis() - t1) + " ms");
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

    Random r = new Random(141221);

    //TODO: Move canonicalize outside the loop
    private RelNode getSubstitution(SqlNode validated, RelNode logicalPlan) {
        String key = getKey(validated);
        List<RelOptMaterialization> possibles = cache.find(key);

        String[] spl = StringUtils.splitByWholeSeparator(key, ",");
        for (String splPart : spl) {
            possibles.addAll(cache.find(splPart));
        }
        for (RelOptMaterialization materialization : possibles) {
            RelNode substituted = optimizer.substitute(materialization, logicalPlan);
            if (substituted != null) {
                return substituted;
            }
        }
        return null;
    }

    private String getKey(SqlNode validated) {
        return String.join(",", QueryUtils.from(validated));
    }

    private RelNode getSubstitution(RelOptMaterialization materialization, RelNode logicalPlan) {
        return optimizer.substitute(materialization, logicalPlan);
    }

    private void runIndividualQuery(String q) {
        System.out.println("Normal exec " + Utils.getPrintableSql(q));
        SqlNode validated = executor.validate(q);
        RelNode logicalPlan = executor.getLogicalPlan(validated);
        RelNode substituted = getSubstitution(validated, logicalPlan);

        if (substituted == null) {
            System.out.println("Creating MV for \n " + Utils.getPrintableSql(q) + "\n");
            RelOptMaterialization materialization = optimizer.materialize(q, logicalPlan);

            long t1 = System.currentTimeMillis();
            long value = cache.getDimension().getType() == Dimension.Type.SIZE_BYTES
                    ? QueryUtils.getTableSize2(q, materialization, executor)
                    : 1;
            subtractable += (System.currentTimeMillis() - t1);
            logTime("Calculating table size took " + (System.currentTimeMillis() - t1) + " ms, Size:" + humanReadable(value));

            cache.add(materialization, getKey(validated), value);
            //TODO: Profile this, is this executed again? If so, find a way to extract results from
            //TODO: materialized table
            executor.execute(getSubstitution(materialization, logicalPlan), rs -> System.out.println("Executed " + q.replace("\n", " ")));
        } else {
            executor.execute(substituted, rs -> System.out.println("MVS Executed " + q.replace("\n", " ")));
        }
    }

    private void runBatchQueries(List<String> queries) {
        for (int i = queries.size() - 1; i >= 0; i--) {
            SqlNode validated = executor.validate(queries.get(i));
            RelNode logical = executor.getLogicalPlan(validated);
            RelNode substituted = getSubstitution(validated, logical);
            if (substituted != null) {
                executor.execute(substituted, rs -> System.out.println("OOB Executed"));
                queries.remove(i);
            }
        }

        System.out.println("Batching queries:");
        for (String query : queries) {
            System.out.println(Utils.getPrintableSql(query) + "\n");
        }

        List<BatchedQuery> batched = batcher.batch(queries);

        // Find out all the queries from the list that couldn't be batched and run them individually
        List<Integer> batchedIndexes = batched.stream().flatMap(bq -> bq.indexes.stream()).toList();
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
            SqlNode validated = executor.validate(bq.sql);
            RelNode plan = executor.getLogicalPlan(validated);
            RelNode substitutable = getSubstitution(validated, plan);
            RelOptMaterialization materialization = null;
            if (substitutable == null) {
                materialization = optimizer.materialize(bq.sql, plan);

                long t1 = System.currentTimeMillis();
                long value = cache.getDimension().getType() == Dimension.Type.SIZE_BYTES
                        ? QueryUtils.getTableSize2(bq.sql, materialization, executor)
                        : 1;
                subtractable += (System.currentTimeMillis() - t1);
                logTime("Calculating table size took " + (System.currentTimeMillis() - t1) + " ms, Size:" + humanReadable(value));

                cache.add(materialization, getKey(validated), value);
            }

            for (SqlNode partQuery : bq.parts) {
                RelNode logicalPlan = executor.getLogicalPlan(partQuery);
                RelNode partSubstitutable = materialization != null
                        ? getSubstitution(materialization, logicalPlan)
                        : getSubstitution(partQuery, logicalPlan);

                if (partSubstitutable == null) {
                    logError("This shouldn't happen!!!!!! Batch query is substitutable but parts are not. Exec query normally");
                    executor.execute(logicalPlan, rs -> System.out.println("Executed " + partQuery.toString()));
                    continue;
                }
                executor.execute(partSubstitutable, rs -> System.out.println("MVS Part Executed " + bq.sql));
                System.out.println();
            }
        }
    }
}
