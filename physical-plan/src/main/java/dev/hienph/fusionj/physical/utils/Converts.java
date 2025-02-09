package dev.hienph.fusionj.physical.utils;

public class Converts {

    public static Boolean toBool(Object t) {
        if (t instanceof Boolean) {
            return (Boolean) t;
        }
        if (t instanceof Number) {
            return (Integer) t == 1;
        }
        throw new IllegalStateException("invalid bool type");
    }

    public static String toString(Object t) {
        if (t instanceof byte[]) {
            return new String((byte[]) t);
        }
        return t.toString();
    }

}
