package dev.hienph.fusionj.fuzzer;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

class EnhancedRandom {
    private final Random rand;
    private final List<Character> charPool;

    public EnhancedRandom(Random rand) {
        this.rand = rand;
        this.charPool = new ArrayList<>();
        for (char c = 'a'; c <= 'z'; c++) charPool.add(c);
        for (char c = 'A'; c <= 'Z'; c++) charPool.add(c);
        for (char c = '0'; c <= '9'; c++) charPool.add(c);
    }

    public byte nextByte() {
        switch (rand.nextInt(5)) {
            case 0:
                return Byte.MIN_VALUE;
            case 1:
                return Byte.MAX_VALUE;
            case 2:
                return 0;
            case 3:
                return 0;
            case 4:
                return (byte) rand.nextInt();
            default:
                throw new IllegalStateException();
        }
    }

    public short nextShort() {
        switch (rand.nextInt(5)) {
            case 0:
                return Short.MIN_VALUE;
            case 1:
                return Short.MAX_VALUE;
            case 2:
                return 0;
            case 3:
                return 0;
            case 4:
                return (short) rand.nextInt();
            default:
                throw new IllegalStateException();
        }
    }

    public int nextInt() {
        switch (rand.nextInt(5)) {
            case 0:
                return Integer.MIN_VALUE;
            case 1:
                return Integer.MAX_VALUE;
            case 2:
                return 0;
            case 3:
                return 0;
            case 4:
                return rand.nextInt();
            default:
                throw new IllegalStateException();
        }
    }

    public long nextLong() {
        switch (rand.nextInt(5)) {
            case 0:
                return Long.MIN_VALUE;
            case 1:
                return Long.MAX_VALUE;
            case 2:
                return 0;
            case 3:
                return 0;
            case 4:
                return rand.nextLong();
            default:
                throw new IllegalStateException();
        }
    }

    public double nextDouble() {
        switch (rand.nextInt(8)) {
            case 0:
                return Double.MIN_VALUE;
            case 1:
                return Double.MAX_VALUE;
            case 2:
                return Double.POSITIVE_INFINITY;
            case 3:
                return Double.NEGATIVE_INFINITY;
            case 4:
                return Double.NaN;
            case 5:
                return -0.0;
            case 6:
                return 0.0;
            case 7:
                return rand.nextDouble();
            default:
                throw new IllegalStateException();
        }
    }

    public float nextFloat() {
        switch (rand.nextInt(8)) {
            case 0:
                return Float.MIN_VALUE;
            case 1:
                return Float.MAX_VALUE;
            case 2:
                return Float.POSITIVE_INFINITY;
            case 3:
                return Float.NEGATIVE_INFINITY;
            case 4:
                return Float.NaN;
            case 5:
                return -0.0f;
            case 6:
                return 0.0f;
            case 7:
                return rand.nextFloat();
            default:
                throw new IllegalStateException();
        }
    }

    public String nextString(int len) {
        StringBuilder result = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            int index = rand.nextInt(charPool.size());
            result.append(charPool.get(index));
        }
        return result.toString();
    }
}

