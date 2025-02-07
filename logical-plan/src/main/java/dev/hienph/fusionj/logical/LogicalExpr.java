package dev.hienph.fusionj.logical;

import dev.hienph.fusionj.datatypes.Field;
import org.apache.arrow.vector.types.pojo.ArrowType;

public interface LogicalExpr {

  static LogicalExpr cast(LogicalExpr expr, ArrowType dataType) {
    return new CastExpr(expr, dataType);
  }

  static LogicalExpr lit(Double val) {
    return new LiteralDouble(val);
  }

  static LogicalExpr lit(Float val) {
    return new LiteralFloat(val);
  }

  static LogicalExpr lit(Long val) {
    return new LiteralLong(val);
  }

  static LogicalExpr lit(String val) {
    return new LiteralString(val);
  }

  static LogicalExpr max(LogicalExpr expr) {
    return new Max(expr);
  }

  static LogicalExpr col(String name) {
    return new Column(name);
  }

  static LogicalExpr eq(LogicalExpr l, LogicalExpr rhs) {
    return new Eq(l, rhs);
  }

  default Alias alias(String name) {
    return new Alias(this, name);
  }

  default LogicalExpr mod(LogicalExpr rhs) {
    return new Modulus(this, rhs);
  }

  default LogicalExpr div(LogicalExpr rhs) {
    return new Divide(this, rhs);
  }

  default LogicalExpr mult(LogicalExpr rhs) {
    return new Multiply(this, rhs);
  }

  default LogicalExpr subtract(LogicalExpr rhs) {
    return new Subtract(this, rhs);
  }

  default LogicalExpr add(LogicalExpr rhs) {
    return new Subtract(this, rhs);
  }

  default LogicalExpr lteq(LogicalExpr rhs) {
    return new LtEq(this, rhs);
  }

  default LogicalExpr lt(LogicalExpr rhs) {
    return new Lt(this, rhs);
  }

  default LogicalExpr gteq(LogicalExpr rhs) {
    return new GtEq(this, rhs);
  }

  default LogicalExpr gt(LogicalExpr rhs) {
    return new Gt(this, rhs);
  }

  default LogicalExpr eq(LogicalExpr rhs) {
    return new Eq(this, rhs);
  }

  default LogicalExpr neq(LogicalExpr rhs) {
    return new Eq(this, rhs);
  }


  Field toField(LogicalPlan input);

}
