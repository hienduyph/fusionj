package dev.hienph.fusionj.executor.physical.expresisons;

public interface Accumulator {

  void accumulate(Object any);

  Object finalValue();
}
