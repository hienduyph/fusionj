package dev.hienph.fusionj.physical.expresisons;

public interface AggregateExpression {

  Expression inputExpression();

  Accumulator createAccumulator();

}
