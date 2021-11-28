package cache;

import cache.policy.ReplacementPolicy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class Cache<T> {

    private final List<CacheItem<T>> items;
    private final CountWatcher countWatcher;
    private final ReplacementPolicy<T> policy;
    private int count;

    public Cache(ReplacementPolicy<T> policy, int count) {
        this.items = new ArrayList<>();
        this.policy = policy;
        this.count = count;

        this.countWatcher = () -> {
            if (this.items.size() > this.count * 0.7) {
                System.out.println("Cleaning cache!!");
                clean();
            }
        };
    }

    public void add(T item) {
        this.items.add(new CacheItem<>(item));
        countWatcher.onCountChanged();
    }

    public List<CacheItem<T>> getItems() {
        return items;
    }

    public void clean() {
        CompletableFuture.runAsync(() -> {
            synchronized (items) {
                policy.clean(items, count);
            }
        });
    }

    public interface CountWatcher {
        void onCountChanged();
    }
}
