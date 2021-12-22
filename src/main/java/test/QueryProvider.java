package test;

import common.Utils;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

public class QueryProvider {

    public List<List<String>> queries;

    private Thread providerThread;

    public QueryProvider() {
        try {
            queries = QueryReader.getQueries(10);
//            for (int i = 0; i < 20; i++) Utils.shuffle(queries);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void listen(Consumer<List<String>> consumer) {
        BlockingQueue<List<String>> blockingQueue = new LinkedBlockingDeque<>();
        providerThread = new Thread(new Provider(this, blockingQueue));
        providerThread.start();

        while (true) {
            try {
                if (providerThread.isInterrupted()) {
                    break;
                }

                consumer.accept(blockingQueue.take());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void stopListening() {
        providerThread.interrupt();
    }

    public static class Provider implements Runnable {
        private final QueryProvider queryProvider;
        private final BlockingQueue<List<String>> queue;
        private int i = 0;

        public Provider(QueryProvider provider, BlockingQueue<List<String>> queue) {
            this.queryProvider = provider;
            this.queue = queue;
        }

        @Override
        public void run() {
            //TODO: Try a different probability distribution maybe?
            while (true) {
                try {
                    Thread.sleep(Utils.getRandomNumber(5 * 1000));
                    List<String> query = queryProvider.queries.get(i);
                    queue.put(query);
                    i++;
                    i %= queryProvider.queries.size();
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }
}
