package test;

import common.Utils;

import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class QueryProvider {

    private static final String b1q1 = """
            SELECT "l_extendedprice", "l_quantity" \
            FROM "public"."lineitem" \
            WHERE "l_shipdate" >= date '1994-01-01' \
            AND "l_shipdate" < date '1994-09-02' \
            AND "l_discount" > 0.07 \
            AND "l_quantity" > 45
            """;

    private static final String b1q2 = """
            SELECT "l_discount" \
            FROM "public"."lineitem"  \
            WHERE "l_shipdate" < date '1994-06-02' \
            AND "l_shipdate" > date '1994-01-01' \
            AND "l_quantity" > 25
            """;

    private static final String b2q1 = """
            SELECT "s_name", "s_suppkey" \
            FROM "public"."supplier", "public"."nation" \
            WHERE "s_nationkey" = "n_nationkey" \
            AND ("s_suppkey" < 800 \
            OR "s_suppkey" > 900)
            """;

    private static final String b2q2 = """
            SELECT "s_name", "s_suppkey" \
            FROM "public"."supplier", "public"."nation" \
            WHERE "s_nationkey" = "n_nationkey" \
            AND "s_suppkey" < 100
            """;

    private static final String b3q1 = """
            SELECT "s_name", "n_name", "r_name" \
            FROM "public"."supplier", "public"."nation", "public"."region" \
            WHERE "s_nationkey" = "n_nationkey" \
            AND "n_regionkey" = "r_regionkey" \
            AND "s_suppkey" < 1200
            """;

    private static final String b3q2 = """
            SELECT "s_name", "n_name", "r_name" \
            FROM "public"."supplier", "public"."nation", "public"."region" \
            WHERE "s_nationkey" = "n_nationkey" \
            AND "n_regionkey" = "r_regionkey" \
            AND "s_suppkey" < 1500
            """;

    private static final String m1q1 = """
            SELECT "l_extendedprice", "l_shipdate", "l_discount", "l_quantity" \
            FROM "public"."lineitem" \
            WHERE "l_shipdate" >= date '1994-01-01' \
            AND "l_shipdate" < date '1995-01-01' \
            AND "l_discount" BETWEEN 0.05 AND 0.08 \
            AND "l_quantity" < 24
            """;

    private static final String m1q2 = """
            SELECT "l_extendedprice" \
            FROM "public"."lineitem" \
            WHERE "l_shipdate" >= date '1994-01-01' \
            AND "l_shipdate" < date '1994-06-01' \
            AND "l_discount" between 0.06 AND 0.07 \
            AND "l_quantity" < 14
            """;

    private static final String m1q3 = """
            SELECT "l_discount" \
            FROM "public"."lineitem" \
            WHERE "l_shipdate" >= date '1994-01-01' \
            AND "l_shipdate" < date '1997-06-01' \
            AND "l_discount" between 0.06 AND 0.07 \
            AND "l_quantity" < 14
            """;

    private static final String m2q1 = """
            SELECT "r_name" \
            FROM "public"."region"
            """;

    private static final String m2q2 = """
            SELECT "r_name" \
            FROM "public"."region" \
            WHERE "r_name" LIKE 'A%'
            """;

    private static final String m3q1 = """
            SELECT "l_discount", "l_quantity" \
            FROM "lineitem" \
            WHERE "l_quantity" < 24
            """;

    private static final String m3q2 = """
            SELECT "l_discount" \
            FROM "public"."lineitem" \
            WHERE "l_quantity" < 4
            """;

    private static final String m4q1 = """
            SELECT "s_suppkey", "s_name" \
            FROM "supplier" \
            WHERE "s_suppkey" < 1000
            """;

    private static final String m4q2 = """
            SELECT "s_name" \
            FROM "public"."supplier" \
            WHERE "s_suppkey" < 100
            """;

    private final Random random = new Random();

    private final List<List<String>> batches = List.of(
            List.of(b1q1, b1q2),
            List.of(b2q1, b2q2),
            List.of(b3q1, b3q2)
    );

    private final List<List<String>> materializables = List.of(
            List.of(m1q1, m1q2, m1q3),
            List.of(m2q1, m2q2),
            List.of(m3q1, m3q2),
            List.of(m4q1, m4q2)
    );

    private Provider provider;
    private Receiver receiver;

    public List<String> getBatch(int index) {
        return batches.get(index);
    }

    public List<String> getAllBatches() {
        return batches.stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    public List<String> getMaterializable(int index) {
        return materializables.get(index);
    }

    public List<String> getAllMaterializables() {
        return materializables.stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    public void listen(Consumer<String> consumer) {
        BlockingQueue<String> blockingQueue = new LinkedBlockingDeque<>();
        provider = new Provider(blockingQueue);
        receiver = new Receiver(provider, blockingQueue, consumer);

        new Thread(provider).start();
        new Thread(receiver).start();
    }

    public void stopListening() {
        provider.stop();
        receiver.stop();
    }

    public static class Provider extends ThreadTask {
        private final BlockingQueue<String> queue;
        private int i = 0;

        public Provider(BlockingQueue<String> queue) {
            this.queue = queue;
        }

        @Override
        public void runUntilStopped() throws InterruptedException {
            Thread.sleep(Utils.getRandomNumber(1 * 1000));
            queue.put(String.valueOf(i));
            i++;
        }
    }

    public static class Receiver extends ThreadTask {
        Provider provider;
        Consumer<String> consumer;
        BlockingQueue<String> queue;

        public Receiver(Provider provider, BlockingQueue<String> queue, Consumer<String> consumer) {
            this.provider = provider;
            this.consumer = consumer;
            this.queue = queue;
        }

        @Override
        public void runUntilStopped() throws InterruptedException {
            String val = queue.take();
            consumer.accept(val);
        }
    }

    abstract static class ThreadTask implements Runnable {
        private boolean shouldStop = false;

        public abstract void runUntilStopped() throws InterruptedException;

        @Override
        public void run() {
            while (!shouldStop) {
                try {
                    runUntilStopped();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        public void stop() {
            this.shouldStop = true;
        }
    }
}
