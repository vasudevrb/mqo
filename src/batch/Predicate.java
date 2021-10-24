package batch;

import org.apache.commons.lang.StringUtils;

import java.util.Objects;

public class Predicate extends Term implements Comparable<Predicate> {
    private String name;
    private String operator;
    private String value;
    private int parsedValue;

    private String shortName = null;

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

    public String getShortName() {
        if (!name.contains(".")) {
            return name;
        }

        return shortName == null ? buildShortName() : shortName;
    }

    private String buildShortName() {
        return name.split("\\.")[1].replaceAll("`", "").replaceAll("\"", "");
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

    public boolean matches(int value) {
        boolean matches = false;

        if (operator.equals(">=")) matches = value >= parsedValue;
        else if (operator.equals("<=")) matches = value <= parsedValue;
        else if (operator.equals(">")) matches = value > parsedValue;
        else if (operator.equals("<")) matches = value < parsedValue;
        else if (operator.equals("=")) matches = value == parsedValue;

        return matches;
    }
    
    public boolean matches(float x) {
        boolean matches = false;
        float pVal = Float.parseFloat(this.value);

        if (operator.equals(">=")) matches = x >= pVal;
        else if (operator.equals("<=")) matches = x <= pVal;
        else if (operator.equals(">")) matches = x > pVal;
        else if (operator.equals("<")) matches = x < pVal;
        else if (operator.equals("=")) matches = x == pVal;

        return matches;
    }


    public boolean matches(Object val) {
        if (isInt(val.toString())) return matches(Integer.parseInt(val.toString()));
        else if (isFloat(val.toString())) return matches(Float.parseFloat(val.toString()));
        else {
            int val2 = Integer.parseInt(StringUtils.replace(val.toString(), "-", "").trim());
            return matches(val2);
        }
    }

    @Override
    public String toString() {
        return name.replace("`", "\"") + " " + operator + " " + value;
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
