package dev.hienph.fusionj.sql;

public enum Literal implements TokenType {
  LONG,
  DOUBLE,
  STRING,
  IDENTIFIER;

  public static Boolean isNumberStart(Character ch) {
    return Character.isDigit(ch) || '.' == ch;
  }

  public static Boolean isIdentifierStart(Character ch) {
    return Character.isLetter(ch);
  }

  public static Boolean isIdentifierPart(Character ch) {
    return Character.isLetter(ch) || Character.isDigit(ch) || ch == '_';
  }

  public static Boolean isCharsStart(Character ch) {
    return '\'' == ch || '"' == ch;
  }

}
