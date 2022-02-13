import batch.QueryBatcher;
import batch.data.BatchedQuery;
import cache.Cache;
import cache.dim.Dimension;
import cache.policy.LRUPolicy;
import cache.policy.ReplacementPolicy;
import common.Configuration;
import common.QueryExecutor;
import common.QueryUtils;
import common.Utils;
import mv.MViewOptimizer;
import mv.Materialization;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import test.QueryReader;

import java.io.IOException;
import java.util.*;

import static common.Logger.logCache;
import static common.Utils.humanReadable;
import static org.apache.commons.io.FileUtils.ONE_GB;

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
                SELECT "l_discount", "l_quantity", "l_shipdate"
                 FROM "public"."lineitem"
                 WHERE "l_shipdate" < date '1994-06-02'
                 AND "l_shipdate" > date '1994-01-01'
                 AND "l_discount" > 0.02
                 AND "l_quantity" > 32
                """;
        String q = """
                SELECT "l_discount"
                 FROM "public"."lineitem"
                 WHERE "l_shipdate" < date '1994-04-02'
                 AND "l_shipdate" > date '1994-03-01'
                 AND "l_discount" > 0.05
                 AND "l_quantity" > 37
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

    public void testBatch2() {
        List<String> queries = new ArrayList<>();

        queries.add("""
                SELECT "l_tax", "l_quantity"
                 FROM "public"."lineitem"
                 WHERE "l_shipdate" < date '1994-06-02'
                 AND "l_shipdate" > date '1994-01-01'
                 AND ("l_discount" > 0.08
                 OR "l_quantity" > 12)
                """);

        queries.add("""
                SELECT "l_quantity", "l_discount"
                FROM "public"."lineitem"
                WHERE "l_shipdate" > date '1994-01-01'
                AND "l_shipdate" < date '1994-06-02'
                AND "l_discount" > 0.02
                AND "l_quantity" > 32
                """);

//        queries.add("""
//                SELECT "ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost"
//               FROM "partsupp"
//               WHERE ("ps_availqty" < 98 AND "ps_partkey" < 98999) OR ("ps_availqty" > 8532 AND "ps_partkey" > 162348)
//                """);

//        queries.add("""
//                SELECT "ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost"
//                FROM "partsupp"
//                WHERE ("ps_availqty" < 98 AND "ps_partkey" < 98999) OR ("ps_availqty" > 8532 AND "ps_partkey" > 162348)
//                """);
//
//        queries.add("""
//                SELECT "ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost"
//                FROM "partsupp"
//                WHERE ("ps_availqty" < 98 AND "ps_partkey" < 98999) OR ("ps_availqty" > 8532 AND "ps_partkey" > 162348)
//                """);

        List<BatchedQuery> bq = new QueryBatcher(config, executor).batch(queries);
        System.out.println(Utils.getPrintableSql(bq.get(0).sql));
    }

    public void printQuerySizes() throws IOException {
        List<Long> sizes = new ArrayList<>();
        List<String> queries = QueryReader.getQueries(20, QueryReader.TYPE_ALL).stream().flatMap(Collection::stream).toList();
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
        List<String> queries = QueryReader.getQueries(10, QueryReader.TYPE_ALL).stream().flatMap(Collection::stream).toList();

        MViewOptimizer op = new MViewOptimizer(config);
        QueryExecutor executor = new QueryExecutor(config);

        //nextDouble > 0.5 = 35
        //nextDouble > 0.8 = 25
        //nextDouble > 0.85 = 20
        //nextDouble > 0.94 = 10

        Random r = new Random(141221);

        Cache<RelOptMaterialization> cache = new Cache<>(new LRUPolicy<>(), Dimension.SIZE(10 * ONE_GB));
        int numDerivable = 0;
        outerloop:
        for (int i = 0; i < queries.size(); i++) {
            System.out.println("=================================");
            logCache("Processing " + i);
            System.out.println(Utils.getPrintableSql(queries.get(i)));
            SqlNode node = executor.validate(queries.get(i));
            String key = getKey(node);

            List<RelOptMaterialization> possibles = cache.find(key);

            String[] spl = StringUtils.splitByWholeSeparator(key, ",");
            for (String splPart : spl) {
                possibles.addAll(cache.find(splPart));
            }

            for (RelOptMaterialization m : possibles) {
                RelNode sub = op.substitute(m, executor.getLogicalPlan(queries.get(i)));
                if (sub != null) {
                    numDerivable++;
                    continue outerloop;
                }
            }

            String mat = queries.get(i);

            cache.add(op.materialize(mat, executor.getLogicalPlan(mat)), key, 1);
        }
        System.out.println();
        System.out.println("Number of derivable: " + numDerivable + ", " + (((double) numDerivable) * 100 / queries.size()) + "%");
    }

    private String getKey(SqlNode validated) {
        return String.join(",", QueryUtils.from(validated));
    }

    public void testDerivabilityPerf() throws IOException {
        List<String> queries = QueryReader.getQueries(1, QueryReader.TYPE_ALL).stream().flatMap(Collection::stream).toList();

        List<SqlNode> validatedNodes = new ArrayList<>();
        for (int i = 0; i < queries.size(); i++) {
            validatedNodes.add(executor.validate(queries.get(i)));
        }

        long t1 = System.currentTimeMillis();
        List<RelOptMaterialization> mats = new ArrayList<>();
        for (int i = 0; i < queries.size(); i++) {
            String query = queries.get(i);

            RelNode logical = executor.getLogicalPlan(validatedNodes.get(i));
            RelOptMaterialization m = optimizer.materialize(query, logical);
            mats.add(m);
        }
        long timeCreateMats = System.currentTimeMillis() - t1;

        t1 = System.currentTimeMillis();
        HashMap<String, List<Integer>> map = new HashMap<>();
        List<Materialization> mats2 = new ArrayList<>();
        for (int i = 0; i < mats.size(); i++) {
            SqlNode validated = validatedNodes.get(i);
            mats2.add(new Materialization(mats.get(i)));

            String key = String.join(",", QueryUtils.from(validated));
            int index = mats2.size() - 1;

            if (map.containsKey(key)) {
                map.get(key).add(index);
            } else {
                var items = new ArrayList<Integer>();
                items.add(index);

                map.put(key, items);
            }
        }
        long timeCreateMap = System.currentTimeMillis() - t1;

        System.out.println("Time taken to materialize: " + timeCreateMats + " ms");
        System.out.println("Additional time for creating a map " + timeCreateMap + " ms");

        for (int i = 0; i < 100; i++) {
            testNormalDerivability(queries, mats);
        }

        DescriptiveStatistics stats = new DescriptiveStatistics();
        for (int i = 0; i < 100; i++) {
            long t2 = System.currentTimeMillis();
            testMapDerivability(queries, mats2, map);
            stats.addValue(System.currentTimeMillis() - t2);
        }

        System.out.println(stats);
        stats.clear();

        for (int i = 0; i < 100; i++) {
            long t2 = System.currentTimeMillis();
            testNormalDerivability(queries, mats);
            stats.addValue(System.currentTimeMillis() - t2);
        }

        System.out.println(stats);
    }

    private void testNormalDerivability(List<String> queries, List<RelOptMaterialization> mats) {
        ql:
        for (int i = queries.size() - 1; i >= 0; i--) {
            String query = queries.get(i);
            RelNode logical = executor.getLogicalPlan(query);
            for (RelOptMaterialization m : mats) {
                if (optimizer.substitute(m, logical) != null) {
                    continue ql;
                }
            }
        }
    }

    private void testMapDerivability(List<String> queries, List<Materialization> mats, HashMap<String, List<Integer>> map) {
        ql:
        for (int i = queries.size() - 1; i >= 0; i--) {
            SqlNode validated = executor.validate(queries.get(i));
            RelNode logical = QueryUtils.canonicalize(executor.getLogicalPlan(validated));
            List<Integer> possible = map.get(String.join(",", QueryUtils.from(validated)));
            if (possible != null && !possible.isEmpty()) {
                for (Materialization m : mats) {
                    if (optimizer.substitute2(m, logical) != null) {
                        continue ql;
                    }
                }
            }
        }
    }

    public void normalExecTest() {
        String mv = """
                SELECT "ps_partkey", "ps_availqty", "ps_supplycost"
                FROM "lineitem" JOIN "partsupp" on "l_partkey" = "ps_partkey"
                WHERE "ps_partkey" < 206 AND "ps_availqty" < 379 AND "ps_supplycost" < 100
                """;

        String q = """
                SELECT "ps_partkey", "ps_availqty", "ps_supplycost"
                FROM "lineitem" JOIN "partsupp" on "l_partkey" = "ps_partkey"
                WHERE "ps_partkey" < 116 AND "ps_availqty" < 269 AND "ps_supplycost" < 7.44
                """;

        long t1 = System.currentTimeMillis();
        executor.execute(executor.getLogicalPlan(q), null);
        System.out.println("Normal phys calc: " + (System.currentTimeMillis() - t1) + " ms");

        RelOptMaterialization m = optimizer.materialize(mv, executor.getLogicalPlan(mv));

        t1 = System.currentTimeMillis();
        executor.execute(optimizer.substitute(m, executor.getLogicalPlan(q)), null);
        System.out.println("Sub phys calc: " + (System.currentTimeMillis() - t1) + " ms");


    }

    public void testCacheSizeMetrics(int size, ReplacementPolicy<RelOptMaterialization> policy) {
        System.out.println("Setting cache size " + size + " MB");
        Window window = new Window(config, size, policy, QueryReader.TYPE_ALL);
        window.run(Main.MODE_HYB);
    }

    public void testMain(int mode, int cacheSizeMB, int queryType) {
        Window window = new Window(config, cacheSizeMB, new LRUPolicy<>(), queryType);
        window.run(mode);
    }
}
