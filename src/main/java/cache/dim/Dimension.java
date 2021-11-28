package cache.dim;

public class Dimension {

    private Type type;
    private long value;

    private Dimension(Type type, long value) {
        this.type = type;
        this.value = value;
    }

    public static Dimension COUNT(int value) {
        return new Dimension(Type.COUNT, value);
    }

    public static Dimension SIZE(long value) {
        return new Dimension(Type.SIZE_BYTES, value);
    }

    public Type getType() {
        return type;
    }

    public long getValue() {
        return value;
    }

    public enum Type {
        COUNT, SIZE_BYTES
    }
}
