package dev.hienph.fusionj.sql;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SqlParserTest {

  @Test
  void math1Test() {
    var expr = parse("1 + 2 * 3");
    System.out.println(expr);
    var expected = new SqlBinaryExpr(new SqlLong(1L), "+",
      new SqlBinaryExpr(new SqlLong(2L), "*", new SqlLong(3L)));
    Assertions.assertEquals(expected, expr);
  }


  private SqlSelect parseSelect(String sql) {
    return (SqlSelect) parse(sql);
  }

  private SqlExpr parse(String sql) {
    System.out.println("parse() " + sql);
    var tokens = new SqlTokenizer(sql).tokenize();
    var parsedQuery = (new SqlParser(tokens)).parse();
    return parsedQuery;
  }
}
