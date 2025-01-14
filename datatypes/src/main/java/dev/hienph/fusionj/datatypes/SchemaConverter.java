package dev.hienph.fusionj.datatypes;

public class SchemaConverter {
  public static Schema fromArrow(org.apache.arrow.vector.types.pojo.Schema arrowSchema) {
    final var fields = arrowSchema.getFields().stream().map(r -> new Field(r.getName(), r.getFieldType().getType())).toList();
    return new Schema(fields);
  }
}
