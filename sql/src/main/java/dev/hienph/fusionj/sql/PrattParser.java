package dev.hienph.fusionj.sql;

import javax.annotation.Nullable;

public interface PrattParser {

  default @Nullable SqlExpr parse(Integer precedent) {
    SqlExpr expr = parsePrefix();
    while (precedent < nextPrecedent()) {
      expr = parseInfix(expr, nextPrecedent());
    }

    return expr;
  }

  default @Nullable SqlExpr parse() {
    return parse(0);
  }

  Integer nextPrecedent();

  @Nullable
  SqlExpr parsePrefix();

  SqlExpr parseInfix(SqlExpr left, Integer precedence);

}
