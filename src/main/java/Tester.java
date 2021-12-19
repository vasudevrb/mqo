import batch.QueryBatcher;
import cache.policy.ReplacementPolicy;
import common.Configuration;
import common.QueryExecutor;
import common.QueryUtils;
import common.Utils;
import mv.MViewOptimizer;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import test.QueryReader;

import java.io.IOException;
import java.util.*;

import static common.Logger.logCache;
import static common.Utils.humanReadable;

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
        /*
         SELECT "l_linenumber", "l_quantity", avg("ps_supplycost"), avg("l_discount")
            FROM "lineitem" JOIN "partsupp" on "l_partkey" = "ps_partkey"
            WHERE "ps_partkey" < 195757 AND "ps_availqty" < 4947 AND "ps_supplycost" < 8.66
            GROUP BY "l_linenumber", "l_quantity"
         */
        String mv = """
                SELECT "o_totalprice", "o_orderkey", "o_custkey", "c_name", "c_acctbal"
                FROM "orders" JOIN "customer" on "o_custkey" = "c_custkey"
                WHERE ("o_totalprice" < 89717.34 AND "c_acctbal" < 300)
                """;
        String q = """
                SELECT "o_totalprice", "o_orderkey", "o_custkey", "c_name", "c_acctbal"
                FROM "orders" JOIN "customer" on "o_custkey" = "c_custkey"
                WHERE ("o_totalprice" < 89717.34 AND "c_acctbal" < 300)
                """;

        //MV execution
        RelOptMaterialization materialization = optimizer.materialize(mv, executor.getLogicalPlan(mv));
        RelNode n = optimizer.substitute(materialization, executor.getLogicalPlan(q));
        if (n != null) {
            System.out.println("Can substitute");
        } else {
            System.out.println("NO!");
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
        List<String> queries = QueryReader.getQueries(20).stream().flatMap(Collection::stream).toList();
        for (int i = 0; i < queries.size(); i++) {
            System.out.println("============================================");
            System.out.println("Executing " + i);
            String query = queries.get(i);
            System.out.println(Utils.getPrintableSql(query));
            RelOptMaterialization m = optimizer.materialize(query, executor.getLogicalPlan(query));
            long size = QueryUtils.getTableSize(query, m, executor);
            m = null;
            sizes.add(size);
            logCache("Size: " + humanReadable(size));
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
        List<String> queries = QueryReader.getQueries(20).stream().flatMap(Collection::stream).toList();

        MViewOptimizer op = new MViewOptimizer(config);
        QueryExecutor executor = new QueryExecutor(config);

        //with der file: 43

        //25%: r.nextInt(7) == 1
        //20%: r.nextInt(10) == 1
        //12%: r.nextInt(20) == 1
        //9.5%: r.nextInt(30) == 1
        //5%: r.nextInt(50) == 1
        //0%: false
        Random r = new Random(141221);

        List<RelOptMaterialization> materializations = new ArrayList<>();
        int numDerivable = 0;
        outerloop:
        for (int i = 0; i < queries.size(); i++) {
            System.out.println("=================================");
            logCache("Processing " + i);
            System.out.println(Utils.getPrintableSql(queries.get(i)));
            for (RelOptMaterialization m : materializations) {
                RelNode sub = op.substitute(m, executor.getLogicalPlan(queries.get(i)));
                if (sub != null) {
                    numDerivable++;
                    continue outerloop;
                }
            }

            String mat = queries.get(i);
//            SqlNode qp = executor.parse(mat);
//            if (QueryUtils.isAggregate(qp)) {
//                mat = executor.deAggregateQuery(qp);
//            }

            materializations.add(op.materialize(mat, executor.getLogicalPlan(mat)));
        }
        System.out.println();
        System.out.println("Number of derivable: " + numDerivable + ", " + (((double) numDerivable) * 100 / queries.size()) + "%");
    }

    public void testCacheSizeMetrics(int size, ReplacementPolicy<RelOptMaterialization> policy) {
        System.out.println("Setting cache size " + size + " MB");
        Window window = new Window(config, size, policy);
        window.run();
    }
}
