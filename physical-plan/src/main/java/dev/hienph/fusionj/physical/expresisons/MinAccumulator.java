package dev.hienph.fusionj.physical.expresisons;

public class MinAccumulator implements Accumulator {

  private Object value = null;

  @Override
  public void accumulate(Object val) {
    if (val == null) {
      return;
    }
    if (this.value == null) {
      this.value = val;
      return;
    }
    boolean isMin = false;
    switch (val) {
      case Byte v -> isMin = v < (Byte) this.value;
      case Short v -> isMin = v < (Short) this.value;
      case Integer v -> isMin = v < (Integer) this.value;
      case Long v -> isMin = v < (Long) this.value;
      case Float v -> isMin = v < (Float) this.value;
      case Double v -> isMin = v < (Double) this.value;
      default -> throw new UnsupportedOperationException("MIN is not implement for other type");
    }
    if (isMin) {
      this.value = val;
    }
  }

  @Override
  public Object finalValue() {
    return value;
  }
}
