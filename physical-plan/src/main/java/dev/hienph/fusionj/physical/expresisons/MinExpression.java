package dev.hienph.fusionj.executor.physical.expresisons;

public record MinExpression(Expression expr) implements AggregateExpression {

  @Override
  public Expression inputExpression() {
    return expr;
  }

  @Override
  public Accumulator createAccumulator() {
    return new MinAccumulator();
  }

  @Override
  public String toString() {
    return String.format("MIN(%s)", expr);
  }
}
