package common;

import org.apache.commons.lang.StringUtils;

public class Utils {

    public static boolean isInt(String val) {
        return StringUtils.isNumeric(val);
    }

    public static boolean isFloat(String str) {
        if (str == null) {
            return false;
        }
        int length = str.length();
        if (length == 0) {
            return false;
        }
        int i = 0;
        if (str.charAt(0) == '-') {
            if (length == 1) {
                return false;
            }
            i = 1;
        }
        for (; i < length; i++) {
            char c = str.charAt(i);
            if ((c < '0' || c > '9') && c != '.') {
                return false;
            }
        }
        return true;
    }

    public static void main(String[] args) {
        System.out.println(isFloat("3.4134"));
    }
}
