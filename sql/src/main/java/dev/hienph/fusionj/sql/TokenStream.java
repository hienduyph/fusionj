package dev.hienph.fusionj.sql;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TokenStream {

  private final List<Token> tokens;
  private Integer i = 0;
  private final Logger log = LoggerFactory.getLogger(TokenStream.class);

  public TokenStream(List<Token> tokens) {
    this.tokens = tokens;
  }

  public List<Token> getTokens() {
    return tokens;
  }

  public @Nullable Token peak() {
    if (i < tokens.size()) {
      return tokens.get(i);
    }
    return null;
  }

  public @Nullable Token next() {
    if (i < tokens.size()) {
      return tokens.get(i++);
    }
    return null;
  }

  public Boolean consumeKeyword(List<String> s) {
    var save = i;
    for (String keyword : s) {
      if (!consumeKeyword(keyword)) {
        i = save;
        return false;
      }
    }
    return true;
  }

  public Boolean consumeKeyword(String s) {
    var peek = peak();
    log.debug("consumeKeyword('{}'), next token is {}", s, peek);
    if (peek == null) {
      return false;
    }
    if (peek.type() instanceof Keyword && peek.text().equals(s)) {
      i++;
      return true;
    }
    return false;
  }

  public Boolean consumeTokenType(TokenType s) {
    var peek = peak();
    if (peek == null) {
      return false;
    }
    if (peek.type() == s) {
      i++;
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return IntStream.range(0, tokens.size())
      .mapToObj(index -> index == i ? "*token" : tokens.toString())
      .collect(Collectors.joining(" "));
  }
}
