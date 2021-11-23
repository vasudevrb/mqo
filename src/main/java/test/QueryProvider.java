package test;

import common.Utils;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

public class QueryProvider {

    private List<List<String>> queries;

    private Thread providerThread;

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
            //TODO: Try gaussian but with rejection sampling
            while (true) {
                try {
                    Thread.sleep(Utils.getRandomNumber(4 * 1000));
                    String query = i == 0 ? queryProvider.getBatch(0).get(0) : queryProvider.getBatch(0).get(1);
                    queue.put(List.of(query));
                    i++;
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }
}