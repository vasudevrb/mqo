package cache.policy;

import cache.CacheItem;

import java.util.List;

public interface ReplacementPolicy<T> {

    default boolean clean(List<CacheItem<T>> items, int maxSize) {
        return clean(items, maxSize, 0.7f);
    }

    boolean clean(List<CacheItem<T>> items, int maxSize, float proportion);
}
