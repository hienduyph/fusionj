package dev.hienph.fusionj.datatypes;

import org.apache.arrow.vector.types.pojo.ArrowType;

public interface ColumnVector {
  ArrowType getType();
  Object getValue(Integer i);
  Integer size();
}
