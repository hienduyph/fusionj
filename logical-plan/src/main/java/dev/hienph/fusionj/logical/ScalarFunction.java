package dev.hienph.fusionj.logical;

import dev.hienph.fusionj.datatypes.Field;
import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class ScalarFunction implements LogicalExpr {

  private final String name;
  private final List<LogicalExpr> args;
  private final ArrowType returnType;

  public ScalarFunction(String name, List<LogicalExpr> args, ArrowType returnType) {
    this.name = name;
    this.args = args;
    this.returnType = returnType;
  }

  @Override
  public Field toField(LogicalPlan input) {
    return new Field(name, returnType);
  }

  @Override
  public String toString() {
    return String.format("%s(%s)", name, args);
  }
}
