package dev.hienph.fusionj.executor.datasource;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface Sequence<T> {

    static <T> Sequence<T> of(Iterator<T> v) {
        return () -> v;
    }

    static <T> Stream<T> stream(Iterator<T> v) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(v, Spliterator.ORDERED), false);
    }

    Iterator<T> iterator();
}
