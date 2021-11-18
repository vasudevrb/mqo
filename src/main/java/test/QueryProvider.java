package test;

import common.Utils;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

public class QueryProvider {

    private List<List<String>> queries;

    private Provider provider;
    private Receiver receiver;

    public QueryProvider() {
        try {
            queries = QueryReader.getQueries();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<String> getBatch(int index) {
        return queries.get(index);
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
            //TODO: Try gaussian but with rejection sampling
            Thread.sleep(Utils.getRandomNumber(4 * 1000));
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
