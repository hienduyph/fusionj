package dev.hienph.fusionj.physical.expresisons;

public class MaxAccumulator implements Accumulator {

  private Object value = null;

  @Override
  public Object finalValue() {
    return value;
  }

  @Override
  public void accumulate(Object value) {
    if (value == null) {
      return;
    }

    if (this.value == null) {
      this.value = value;
    }
    boolean isMax = false;
    switch (value) {
      case Byte v -> isMax = v > (Byte) this.value;
      case Short v -> isMax = v > (Short) this.value;
      case Integer v -> isMax = v > (Integer) this.value;
      case Long v -> isMax = v > (Long) this.value;
      case Float v -> isMax = v > (Float) this.value;
      case Double v -> isMax = v > (Double) this.value;
      case String v -> isMax = v.compareTo((String) this.value) > 0;
      default ->
          throw new IllegalStateException(String.format("MAX is not implemented for data type %s",
              value.getClass().getName()));
    }
    if (isMax) {
      this.value = value;
    }
  }
}
