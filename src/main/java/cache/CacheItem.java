package cache;

public class CacheItem<T> {
    private T item;

    // This 'value' is based on what the evaluatable dimension of the cache is.
    // If dimension is SIZE, then value is the size in bytes of the object T.
    // If dimension is COUNT, then value is 1.
    private long value;

    private long lastAccessTime;
    private int numAccesses;

    public CacheItem(T item) {
        this.item = item;
        this.lastAccessTime = System.currentTimeMillis();
    }

    public CacheItem(T item, long value) {
        this.item = item;
        this.value = value;
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
}
