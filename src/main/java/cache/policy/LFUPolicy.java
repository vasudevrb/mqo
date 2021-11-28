package cache.policy;

import cache.CacheItem;

import java.util.List;

public class LFUPolicy<T> implements ReplacementPolicy<T> {
    @Override
    public boolean clean(List<CacheItem<T>> cacheItems) {
        return false;
    }
}
