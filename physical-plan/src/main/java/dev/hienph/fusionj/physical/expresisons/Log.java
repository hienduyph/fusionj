package dev.hienph.fusionj.executor.physical.expresisons;

public class Log extends UnaryMathExpression {

  public Log(Expression expr) {
    super(expr);
  }

  @Override
  Double apply(Double value) {
    return Math.log(value);
  }
}
