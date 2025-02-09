package dev.hienph.fusionj.physical.expresisons;

public interface Accumulator {

  void accumulate(Object any);

  Object finalValue();
}
