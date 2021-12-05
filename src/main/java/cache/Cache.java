package cache;

import cache.dim.Dimension;
import cache.policy.ReplacementPolicy;
import org.apache.commons.io.FileUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class Cache<T> {

    private final List<CacheItem<T>> items;
    private final Dimension dimension;
    private final ReplacementPolicy<T> policy;

    private final CacheSizeWatcher cacheSizeWatcher;

    // This variable depends on the dimension that the cache is evaluated on.
    // If dimension is SIZE, then this value is the max allowed size in bytes.
    // If dimension is COUNT, then this value is the size of the list items.
    private long currentCacheSize;

    public Cache(ReplacementPolicy<T> policy, Dimension dimension) {
        this.items = new ArrayList<>();
        this.policy = policy;
        this.dimension = dimension;

        this.cacheSizeWatcher = () -> {
            if (currentCacheSize > this.dimension.getValue() * ReplacementPolicy.PRUNE_THRESHOLD) {
                System.out.println("Cleaning cache!!");
                clean();
            }
        };
    }

    public synchronized void add(T item, long value) {
        this.items.add(new CacheItem<>(item, value));
        currentCacheSize += value;
        System.out.println("Added item: Cache size is " + formatCacheSize());
        cacheSizeWatcher.onCacheSizeChange();
    }

    public List<CacheItem<T>> getItems() {
        return items;
    }

    public void clean() {
        CompletableFuture.runAsync(() -> {
            synchronized (items) {
                currentCacheSize = policy.clean(items, currentCacheSize, dimension);
                System.out.println("After clean: current size is " + formatCacheSize() + ", length: " + items.size());
            }
        });
    }

    private String formatCacheSize() {
        return dimension.getType() == Dimension.Type.COUNT
                ? String.valueOf(currentCacheSize) :
                FileUtils.byteCountToDisplaySize(currentCacheSize);
    }

    public interface CacheSizeWatcher {
        void onCacheSizeChange();
    }

    public Dimension getDimension() {
        return dimension;
    }
}
