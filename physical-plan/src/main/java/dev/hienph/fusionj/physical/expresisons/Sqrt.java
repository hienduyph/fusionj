package dev.hienph.fusionj.executor.physical.expresisons;

public class Sqrt extends UnaryMathExpression {

  public Sqrt(Expression expr) {
    super(expr);
  }

  @Override
  Double apply(Double value) {
    return Math.sqrt(value);
  }
}
