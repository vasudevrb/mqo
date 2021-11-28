package cache.policy;

import cache.CacheItem;

import java.util.List;

public interface ReplacementPolicy<T> {
    boolean clean(List<CacheItem<T>> items);
}
