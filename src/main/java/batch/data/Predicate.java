package batch.data;

import common.Utils;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

public class Predicate extends Term implements Comparable<Predicate> {
    private String name;
    private String operator;
    private String value;
    private int parsedValue;

    private String shortName = null;
    private String unquotedName = null;

    private boolean isInt;
    private boolean isFloat;

    public boolean isJoin;

    public Predicate(String name, String operator, String value) {
        this.name = name;
        this.operator = operator;
        this.value = StringUtils.replace(value, "`", "");
        this.value = StringUtils.replace(this.value, "\"", "");

        this.unquotedName = StringUtils.replace(name, "`", "");
        this.unquotedName = StringUtils.replace(unquotedName, "\"", "");

        if (value.startsWith("DATE")) {
            parsedValue = Integer.parseInt(value.replace("DATE", "").replace("'", "").replace("-", "").trim());
        } else if (isInt(value)) {
            parsedValue = Integer.parseInt(value);
        } else if (isFloat(value)) {
            isFloat = true;
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
        shortName = StringUtils.replace(StringUtils.split(name, ".")[1], "`", "");
        shortName = StringUtils.replace(shortName, "\"", "");
        return shortName;
    }

    @Override
    public int compareTo(Predicate predicate) {
        return isFloat
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
        isInt = Utils.isInt(val);
        return isInt;
    }

    private boolean isFloat(String val) {
        try {
            Float.parseFloat(val);
            isFloat = true;
        } catch (NumberFormatException e) {
            isFloat = false;
        }
        return isFloat;
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
        if (isJoin) return true;
        else if (Utils.isInt(val.toString())) return matches(Integer.parseInt(val.toString()));
        else if (Utils.isFloat(val.toString())) return matches(Float.parseFloat(val.toString()));
        else {
            int val2 = Integer.parseInt(StringUtils.replace(val.toString(), "-", "").trim());
            return matches(val2);
        }
    }

    @Override
    public String toString() {
        return unquotedName + " " + operator + " " + value;
    }

    public String buildString() {
        return name + " " + operator + " " + (isJoin ? Utils.placeQuotes(value) : value);
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
