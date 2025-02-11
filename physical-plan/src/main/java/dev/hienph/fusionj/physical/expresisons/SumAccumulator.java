package dev.hienph.fusionj.physical.expresisons;

public class SumAccumulator implements Accumulator {

  private Object value = null;

  @Override
  public Object finalValue() {
    return value;
  }


  @Override
  public void accumulate(Object val) {
    if (val == null) {
      return;
    }
    if (this.value == null) {
      this.value = val;
    }
    var currValue = this.value;
    switch (currValue) {
      case Byte v -> this.value = v + (Byte) val;
      case Short v -> this.value = v + (Short) val;
      case Integer v -> this.value = v + (Integer) value;
      case Long v -> this.value = v + (Long) value;
      case Float v -> this.value = v + (Float) value;
      case Double v -> this.value = v + (Double) value;
      default -> throw new UnsupportedOperationException(
          String.format("MIN is not implemented yet for the type: %s", value.getClass().getName()));
    }

  }
}
