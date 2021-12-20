package cache;

import java.util.Objects;

public class CacheItem<T> {
    private T item;

    // This 'value' is based on what the evaluatable dimension of the cache is.
    // If dimension is SIZE, then value is the size in bytes of the object T.
    // If dimension is COUNT, then value is 1.
    private long value;

    private long lastAccessTime;
    private int numAccesses;

    // To keep track of hashmap keys
    private String key;

    public CacheItem(T item, String key, long value) {
        this.item = item;
        this.value = value;
        this.key = key;
        this.lastAccessTime = System.currentTimeMillis();
    }

    public T getItem() {
        lastAccessTime = System.currentTimeMillis();
        numAccesses++;
        return item;
    }

    public long getValue() {
        return value;
    }

    public String getKey() {
        return key;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public void incrementNumAccesses() {
        numAccesses++;
    }

    public int getNumAccesses() {
        return numAccesses;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CacheItem<?> cacheItem = (CacheItem<?>) o;
        return value == cacheItem.value && lastAccessTime == cacheItem.lastAccessTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, lastAccessTime);
    }
}
