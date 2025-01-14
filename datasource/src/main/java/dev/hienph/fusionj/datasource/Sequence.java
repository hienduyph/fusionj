package dev.hienph.fusionj.datasource;

import java.util.Iterator;

public interface Sequence<T> {

  static <T> Sequence<T> of(Iterator<T> v) {
    return () -> v;
  }

  Iterator<T> iterator();
}
