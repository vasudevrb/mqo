package cache.policy;

import cache.CacheItem;
import cache.dim.Dimension;

import java.util.List;
import java.util.stream.IntStream;

public class LFUPolicy<T> implements ReplacementPolicy<T> {

    @Override
    public long clean(List<CacheItem<T>> cacheItems, long currentSize, Dimension dimension, float proportion) {
        int newSize = (int) (dimension.getValue() * proportion);

        while (currentSize > newSize) {
            int minIdx = IntStream.range(0, cacheItems.size())
                    .reduce((i, j) -> cacheItems.get(i).getNumAccesses() > cacheItems.get(j).getNumAccesses() ? j : i)
                    .getAsInt();

            CacheItem<T> item = cacheItems.get(minIdx);
            currentSize -= item.getValue();
            System.out.println("Removing cache index " + minIdx);
            cacheItems.remove(minIdx);
        }

        return currentSize;
    }
}
