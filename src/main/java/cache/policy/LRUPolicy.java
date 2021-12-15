package cache.policy;

import cache.CacheItem;
import cache.dim.Dimension;

import java.util.List;
import java.util.stream.IntStream;

import static common.Logger.logCache;

public class LRUPolicy<T> implements ReplacementPolicy<T> {

    @Override
    public long clean(List<CacheItem<T>> cacheItems, long currentSize, Dimension dimension, float proportion) {
        int newSize = (int) (dimension.getValue() * proportion);
        logCache("Cache clean called when cache size is " + currentSize + ", num items: " + cacheItems.size());
//        System.exit(0);

        while (currentSize > newSize) {
            int minIdx = IntStream.range(0, cacheItems.size())
                    .reduce((i, j) -> cacheItems.get(i).getLastAccessTime() > cacheItems.get(j).getLastAccessTime() ? j : i)
                    .getAsInt();

            CacheItem<T> item = cacheItems.get(minIdx);
            currentSize -= item.getValue();
            cacheItems.remove(minIdx);
        }

        return currentSize;
    }

    @Override
    public String toString() {
        return "LRU";
    }
}
