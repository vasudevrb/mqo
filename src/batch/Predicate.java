package batch;

import java.util.Objects;

public class Predicate extends Term implements Comparable<Predicate> {
    private String name;
    private String operator;
    private String value;
    private int parsedValue;


    public Predicate(String name, String operator, String value) {
        this.name = name;
        this.operator = operator;
        this.value = value;

        if (value.startsWith("DATE")) {
            parsedValue = Integer.parseInt(value.replace("DATE", "").replace("'", "").replace("-", "").trim());
        } else if (isInt(value)) {
            parsedValue = Integer.parseInt(value);
        }
    }

    public static Predicate newPredicate(String name, String operator, String value) {
        return new Predicate(name, operator, value);
    }

    public static Predicate copy(Predicate predicate) {
        return new Predicate(predicate.getName(), predicate.getOperator(), predicate.getValue());
    }

    public Predicate copyWithOp(String op) {
        return new Predicate(name, op, value);
    }

    public boolean isOperator(String op) {
        return this.getOperator().equals(op);
    }

    public String getName() {
        return name;
    }

    @Override
    public int compareTo(Predicate predicate) {
        return isFloat(value)
                ? Float.compare(Float.parseFloat(value), Float.parseFloat(predicate.value))
                : Integer.compare(this.parsedValue, predicate.parsedValue);
    }

    public String getOperator() {
        return operator;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    private boolean isInt(String val) {
        try {
            Integer.parseInt(val);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    private boolean isFloat(String val) {
        try {
            Float.parseFloat(val);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return name + " " + operator + " " + value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return toString().equals(o.toString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, operator, value, parsedValue);
    }
}
