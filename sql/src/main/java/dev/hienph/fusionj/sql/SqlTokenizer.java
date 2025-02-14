package dev.hienph.fusionj.sql;

import dev.hienph.fusionj.sql.utils.CharsUtils;
import java.util.ArrayList;
import java.util.Optional;
import javax.annotation.Nullable;

/*
Tokenizer or Lexer the sql string
 */
public class SqlTokenizer {

  private final String sql;
  private Integer offset = 0;

  public SqlTokenizer(String sql) {
    this.sql = sql;
  }

  public TokenStream tokenize() {
    var token = nextToken();
    final var list = new ArrayList<Token>();
    while (token != null) {
      list.add(token);
      token = nextToken();
    }
    return new TokenStream(list);
  }

  private @Nullable Token nextToken() {
    offset = skipWhiteSpace(offset);
    Token token = null;
    if (offset >= sql.length()) {
      return token;
    } else if (Literal.isIdentifierStart(sql.charAt(offset))) {
      token = scanIdentifier(offset);
      offset = token.endOffset();
    } else if (Literal.isNumberStart(sql.charAt(offset))) {
      token = scanNumber(offset);
      offset = token.endOffset();
    } else if (Symbol.isSymbolStart(sql.charAt(offset))) {
      token = scanSymbol(offset);
      offset = token.endOffset();
    } else if (Literal.isCharsStart(sql.charAt(offset))) {
      token = scanChars(offset, sql.charAt(offset));
      offset = token.endOffset();
    }
    return token;
  }

  /*
  find next offset of non-white space character
   */
  private Integer skipWhiteSpace(Integer startOffset) {
    return CharsUtils.indexOfFirst(sql, startOffset, c -> !Character.isWhitespace(c));
  }


  private Token scanNumber(Integer startOffset) {
    // check negative number
    var endOffset =
      ('-' == sql.charAt(startOffset)) ? CharsUtils.indexOfFirst(sql, startOffset + 1,
        ch -> !Character.isDigit(ch)) :
        CharsUtils.indexOfFirst(sql, startOffset, ch -> !Character.isDigit(ch));
    if (endOffset == sql.length()) {
      return new Token(sql.substring(startOffset, endOffset), Literal.LONG, endOffset);
    }
    var isFloat = '.' == sql.charAt(endOffset);
    if (isFloat) {
      endOffset = CharsUtils.indexOfFirst(sql, endOffset + 1, ch -> !Character.isDigit(ch));
    }
    return new Token(sql.substring(startOffset, endOffset), isFloat ? Literal.DOUBLE : Literal.LONG
      , endOffset);
  }


  private Token scanIdentifier(Integer startOffset) {
    if ('`' == sql.charAt(startOffset)) {
      var endOffset = getOffsetUntilTerminatedChar('`', startOffset);
      return new Token(sql.substring(startOffset, endOffset), Literal.IDENTIFIER, endOffset);
    }
    var endOffset = CharsUtils.indexOfFirst(sql, startOffset, ch -> !Literal.isIdentifierPart(ch));
    var text = sql.substring(startOffset, endOffset);
    if (isAmbiguousIdentifier(text)) {
      return new Token(text, processAmbigousIdentifier(endOffset, text), endOffset);
    }
    var tokenType = Optional.ofNullable((TokenType) Keyword.textOf(text))
      .orElse(Literal.IDENTIFIER);
    return new Token(text, tokenType, endOffset);
  }

  private Integer getOffsetUntilTerminatedChar(Character ch, Integer startOffset) {
    var offset = sql.indexOf(ch, startOffset);
    if (offset != -1) {
      return offset;
    }
    throw new RuntimeException("Must contains terminate chars in offset");
  }

  private Boolean isAmbiguousIdentifier(String text) {
    return Keyword.ORDER.name().equals(text) || Keyword.GROUP.name().equals(text);
  }

  private TokenType processAmbigousIdentifier(Integer startOffset, String text) {
    var skipWhiteSpaceOffset = skipWhiteSpace(startOffset);
    if (skipWhiteSpaceOffset != sql.length() && Keyword.BY.name()
      .equals(sql.substring(skipWhiteSpaceOffset, skipWhiteSpaceOffset + 2))) {
      return Keyword.textOf(text);
    }
    return Literal.IDENTIFIER;
  }

  private Token scanSymbol(Integer startOffset) {
    var endOffset = CharsUtils.indexOfFirst(sql, startOffset, ch -> !Symbol.isSymbol(ch));
    var text = sql.substring(startOffset, endOffset);
    while (null == Symbol.textOf(text)) {
      text = sql.substring(offset, --endOffset);
    }
    Symbol symbol = Symbol.textOf(text);
    if (symbol == null) {
      throw new RuntimeException(String.format("%s must be symbol", text));
    }
    return new Token(text, symbol, endOffset);
  }

  private Token scanChars(Integer startOffset, Character ch) {
    var endOffset = getOffsetUntilTerminatedChar(ch, startOffset + 1);
    return new Token(sql.substring(startOffset, endOffset), Literal.STRING, endOffset + 1);
  }
}
