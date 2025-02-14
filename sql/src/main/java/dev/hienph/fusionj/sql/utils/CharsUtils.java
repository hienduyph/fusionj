package dev.hienph.fusionj.sql.utils;

import java.util.function.Predicate;
import java.util.stream.IntStream;

public class CharsUtils {

  public static Integer indexOfFirst(String text, Predicate<Character> predicate) {
    return indexOfFirst(text, 0, predicate);
  }

  public static Integer indexOfFirst(String text, Integer startOffset,
    Predicate<Character> predicate) {
    return IntStream.range(startOffset, text.length())
      .filter(idx -> predicate.test(text.charAt(idx)))
      .findFirst()
      .orElse(text.length());
  }
}
