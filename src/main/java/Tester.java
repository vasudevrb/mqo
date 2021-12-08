import batch.QueryBatcher;
import cache.policy.ReplacementPolicy;
import common.*;
import mv.MViewOptimizer;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import test.QueryReader;

import java.io.IOException;
import java.util.*;

public class Tester {

    private final QueryExecutor executor;
    private final Configuration config;
    private final MViewOptimizer optimizer;

//    private final QueryProvider queryProvider;

    public Tester(Configuration config) {
        this.optimizer = new MViewOptimizer(config);
        this.executor = new QueryExecutor(config);
        this.config = config;
//        this.queryProvider = new QueryProvider();
    }

    public void testMVSubstitution() throws Exception {
//        List<String> matQueries = queryProvider.getBatch(4);
        List<String> matQueries = new ArrayList<>();

        //Regular execution
        RelNode regNode = executor.getLogicalPlan(matQueries.get(0));
        executor.execute(regNode, null);

        //MV execution
        RelOptMaterialization materialization = optimizer.materialize("mv0", matQueries.get(0));
        RelNode n = optimizer.substitute(materialization, executor.getLogicalPlan(matQueries.get(1)));
        if (n != null) {
            executor.execute(n, null);
        }
    }

    public void testBatch() {
        QueryBatcher queryBatcher = new QueryBatcher(config, executor);

//        long t1 = System.currentTimeMillis();
//        List<BatchedQuery> combined = queryBatcher.batch(queryProvider.getBatch(2));
//        for (BatchedQuery bq : combined) {
//            executor.execute(executor.getLogicalPlan(bq.sql), rs -> System.out.println("Row count: " + QueryUtils.countRows(rs)));
//        }
//        System.out.println("Took " + (System.currentTimeMillis() - t1) + "ms");


        ArrayList<Long> times = new ArrayList<>();
        RelOptMaterialization m = null;
        Map<String, RelNode> exec = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            long t1 = System.currentTimeMillis();

            //Serial
            for (String q : new ArrayList<String>()) {
                executor.execute(executor.getLogicalPlan(q), rs -> System.out.println("Row count: " + QueryUtils.countRows(rs)));
            }

            //Batch
//            if (m == null) {
//                List<BatchedQuery> combined = queryBatcher.batch(queryProvider.getBatch(2));
//                m = optimizer.materialize("asdasf", combined.get(0).sql);
//            }
//            for (String q: queryProvider.getBatch(2)) {
//                RelNode e;
//                if (exec.containsKey(q)) {
//                    System.out.println("Found in map");
//                    e = exec.get(q);
//                } else {
//                    e = optimizer.substitute(m, executor.getLogicalPlan(q));
//                    exec.put(q, e);
//                }
//
//                executor.execute(e, rs -> System.out.println("RC: " + QueryUtils.countRows(rs)));
//            }

            times.add(System.currentTimeMillis() - t1);
        }

        DescriptiveStatistics stats = new DescriptiveStatistics();
        times.forEach(stats::addValue);

        System.out.println(stats);
    }

    public void printQuerySizes() throws IOException {
        List<Long> sizes = new ArrayList<>();
        List<String> queries = QueryReader.getQueries(20);
        for (int i = 0; i < queries.size(); i++) {
            System.out.println("============================================");
            System.out.println("Executing " + i);
            String query = queries.get(i);
            System.out.println(Utils.getPrintableSql(query));
            RelOptMaterialization m = optimizer.materialize(Utils.randomString(5), query);
            long size = QueryUtils.getTableSize(query, m, executor);
            m = null;
            sizes.add(size);
            Logger.logCache("Size: " + FileUtils.byteCountToDisplaySize(size));
        }

        System.out.println("Unsorted byte sizes");
        System.out.println(sizes);

        Collections.sort(sizes);
        List<String> sizeStrings = sizes.stream().map(FileUtils::byteCountToDisplaySize).toList();
        System.out.println("Sorted readable sizes");
        System.out.println(sizeStrings);
    }

    public void testFindDerivablePercentage() throws IOException {
        boolean deAgg = true;
        List<String> queries = QueryReader.getQueries(20);

        MViewOptimizer op = new MViewOptimizer(config);
        QueryExecutor executor = new QueryExecutor(config);

        List<RelOptMaterialization> materializations = new ArrayList<>();
        int numDerivable = 0;
        Utils.shuffle(queries);
        outerloop:
        for (int i = 0; i < queries.size(); i++) {
            System.out.print("\rProcessing " + i);
            for (RelOptMaterialization m : materializations) {
                RelNode sub = op.substitute(m, executor.getLogicalPlan(queries.get(i)));
                if (sub != null) {
                    numDerivable++;
                    continue outerloop;
                }
            }

            String mat = queries.get(i);
            SqlNode qp = executor.parse(mat);
            if (QueryUtils.isAggregate(qp)) {
                mat = executor.deAggregateQuery(qp);
            }

            materializations.add(op.materialize(Utils.randomString(7), mat));
        }
        System.out.println();
        System.out.println("Number of derivable: " + numDerivable);
    }

    public void testCacheSizeMetrics(int size, ReplacementPolicy<RelOptMaterialization> policy) {
        System.out.println("Setting cache size " + size + " MB");
        Window window = new Window(config, size, policy);
        window.run();
    }
}
