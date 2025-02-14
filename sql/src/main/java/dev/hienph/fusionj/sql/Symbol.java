package dev.hienph.fusionj.sql;


import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public enum Symbol implements TokenType {
  LEFT_PAREN("("),
  RIGHT_PAREN(")"),
  LEFT_BRACE("{"),
  RIGHT_BRACE("}"),
  LEFT_BRACKET("["),
  RIGHT_BRACKET("]"),
  SEMI(";"),
  COMMA(","),
  DOT("."),
  DOUBLE_DOT(".."),
  PLUS("+"),
  SUB("-"),
  STAR("*"),
  SLASH("/"),
  QUESTION("?"),
  EQ("="),
  GT(">"),
  LT("<"),
  BANG("!"),
  TILDE("~"),
  CARET("^"),
  PERCENT("%"),
  COLON(":"),
  DOUBLE_COLON("::"),
  COLON_EQ(":="),
  LT_EQ("<="),
  GT_EQ(">="),
  LT_EQ_GT("<=>"),
  LT_GT("<>"),
  BANG_EQ("!="),
  BANG_GT("!>"),
  BANG_LT("!<"),
  AMP("&"),
  BAR("|"),
  DOUBLE_AMP("&&"),
  DOUBLE_BAR("||"),
  DOUBLE_LT("<<"),
  DOUBLE_GT(">>"),
  AT("@"),
  POUND("#");

  private String text;

  public String getText() {
    return text;
  }

  Symbol(String text) {
    this.text = text;
  }


  private static final Map<String, Symbol> symbols = Arrays.stream(values())
    .collect(Collectors.toMap(Symbol::getText, it -> it));
  private static final Set<Character> symbolsStartSet = Arrays.stream(values())
    .flatMap(s -> s.text.chars().mapToObj(c -> (char) c)).collect(Collectors.toSet());

  public static Symbol textOf(String text) {
    return symbols.get(text);
  }

  public static Boolean isSymbol(Character ch) {
    return symbolsStartSet.contains(ch);
  }

  public static Boolean isSymbolStart(Character ch) {
    return isSymbol(ch);
  }

}
