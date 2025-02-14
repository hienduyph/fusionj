package dev.hienph.fusionj.executor.physical.expresisons;

public interface AggregateExpression {

  Expression inputExpression();

  Accumulator createAccumulator();

}
