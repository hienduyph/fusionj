package dev.hienph.fusionj.datatypes;

import java.util.List;

public record Schema(
    List<Field> fields
) {

  public org.apache.arrow.vector.types.pojo.Schema toArrow() {
    return new org.apache.arrow.vector.types.pojo.Schema(
        fields.stream().map(Field::toArrow).toList());
  }

  public Schema project(List<Integer> indices) {
    return new Schema(indices.stream().map(fields::get).toList());
  }

  public Schema select(List<String> names) {
    final var f = names.stream()
        .map(name -> fields.stream().filter(ff -> ff.name().equals(name)).findFirst().orElseThrow(
            IllegalArgumentException::new)).toList();
    return new Schema(f);
  }
}
