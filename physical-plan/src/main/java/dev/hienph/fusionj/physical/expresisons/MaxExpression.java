package dev.hienph.fusionj.executor.physical.expresisons;

public record MaxExpression(Expression expr) implements AggregateExpression {

  @Override
  public Expression inputExpression() {
    return expr;
  }

  @Override
  public Accumulator createAccumulator() {
    return new MaxAccumulator();
  }

  @Override
  public String toString() {
    return String.format("MAX(%s)", expr);
  }
}
