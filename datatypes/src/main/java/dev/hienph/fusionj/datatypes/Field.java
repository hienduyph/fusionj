package dev.hienph.fusionj.datatypes;

import java.util.ArrayList;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

public record Field(
    String name,
    ArrowType dataType
) {
  public org.apache.arrow.vector.types.pojo.Field toArrow() {
    final var fieldType = new FieldType(true, dataType, null);
    return new org.apache.arrow.vector.types.pojo.Field(name, fieldType, new ArrayList<>());
  }

}
