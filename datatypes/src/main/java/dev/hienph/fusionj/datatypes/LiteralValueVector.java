package dev.hienph.fusionj.datatypes;

import org.apache.arrow.vector.types.pojo.ArrowType;

public record LiteralValueVector(ArrowType arrowType, Object value, Integer size)  implements ColumnVector {

  @Override
  public ArrowType getType() {
    return  arrowType;
  }

  @Override
  public Object getValue(Integer i) {
    if (i < 0 || i >= size) {
      throw new IndexOutOfBoundsException();
    }
    return value;
  }

  @Override
  public Integer size() {
    return size;
  }
}


