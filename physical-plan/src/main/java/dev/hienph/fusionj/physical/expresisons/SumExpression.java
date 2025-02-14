package dev.hienph.fusionj.executor.physical.expresisons;

public record SumExpression(Expression expr) implements AggregateExpression {

  @Override
  public Expression inputExpression() {
    return expr;
  }

  @Override
  public Accumulator createAccumulator() {
    return new SumAccumulator();
  }

  @Override
  public String toString() {
    return String.format("SUM(%s)", expr);
  }
}
