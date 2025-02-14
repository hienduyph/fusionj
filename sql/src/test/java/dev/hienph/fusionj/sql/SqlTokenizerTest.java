package dev.hienph.fusionj.sql;

import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SqlTokenizerTest {

  @Test
  void tokenizeSimpleSelect() {
    var actual = new SqlTokenizer("SELECT id, first_name, last_name FROM employee").tokenize()
      .getTokens();
    var expected = List.of(
      new Token("SELECT", Keyword.SELECT, 6),
      new Token("id", Literal.IDENTIFIER, 9),
      new Token(",", Symbol.COMMA, 10),
      new Token("first_name", Literal.IDENTIFIER, 21),
      new Token(",", Symbol.COMMA, 22),
      new Token("last_name", Literal.IDENTIFIER, 32),
      new Token("FROM", Keyword.FROM, 37),
      new Token("employee", Literal.IDENTIFIER, 46)
    );
    Assertions.assertEquals(expected, actual);
  }
}
