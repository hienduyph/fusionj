package dev.hienph.fusionj.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlParser implements PrattParser {

  private final TokenStream tokens;
  private static final Logger log = LoggerFactory.getLogger(SqlParser.class);

  public SqlParser(TokenStream tokenStream) {
    this.tokens = tokenStream;
  }

  @Override
  public Integer nextPrecedent() {
    var token = Optional.ofNullable(tokens.peak());
    if (token.isEmpty()) {
      return 0;
    }
    var precedence = switch (token.get().type()) {
      case Keyword.ASC, Keyword.AS, Keyword.DESC -> 10;
      case Keyword.OR -> 20;
      case Keyword.AND -> 30;
      // symbols
      case Symbol.LT, Symbol.LT_EQ, Symbol.EQ, Symbol.BANG_EQ, Symbol.GT_EQ, Symbol.GT -> 40;
      case Symbol.PLUS, Symbol.SUB -> 50;
      case Symbol.STAR, Symbol.SLASH -> 60;
      case Symbol.LEFT_PAREN -> 70;
      default -> 0;
    };
    log.info("NextPrecedence({}) returning {}", token, precedence);
    return precedence;
  }

  @Override
  public @Nullable SqlExpr parsePrefix() {
    log.info("parsePrefix()  next token {}", tokens.peak());
    var token = tokens.next();
    if (token == null) {
      return null;
    }
    var expr = switch (token.type()) {
      case Keyword.SELECT -> parseSelect();
      case Keyword.CAST -> parseCast();
      case Keyword.MAX, Keyword.INT, Keyword.DOUBLE, Literal.IDENTIFIER, Literal.STRING ->
        new SqlIdentifier(token.text());
      case Literal.LONG -> new SqlLong(Long.parseLong(token.text()));
      case Literal.DOUBLE -> new SqlDouble(Double.parseDouble(token.text()));
      default -> throw new IllegalStateException(String.format("Unexpected token %s", token));
    };
    return expr;
  }

  @Override
  public SqlExpr parseInfix(SqlExpr left, Integer precedence) {
    log.info("parseInfix() next token {}", tokens.peak());
    var token = tokens.peak();
    if (token == null) {
      throw new IllegalStateException("Expected have a valid token, got null");
    }
    var expr = switch (token.type()) {
      case Symbol.PLUS, Symbol.SUB, Symbol.STAR, Symbol.SLASH, Symbol.EQ, Symbol.GT, Symbol.LT,
           Keyword.AND, Keyword.OR -> {
        tokens.next();
        yield new SqlBinaryExpr(left, token.text(), parse(precedence));
      }
      case Keyword.AS -> {
        tokens.next();
        yield new SqlAlias(left, parseIdentifier());
      }
      case Keyword.ASC, Keyword.DESC -> {
        tokens.next();
        yield new SqlSort(left, token.type().equals(Keyword.ASC));
      }
      case Symbol.LEFT_PAREN -> {
        if (left instanceof SqlIdentifier) {
          tokens.next();
          var ars = parseExprList();
          if (tokens.next() == null || !tokens.next().type().equals(Symbol.RIGHT_PAREN)) {
            throw new IllegalStateException(String.format("Require right paren"));
          }
          yield new SqlFunction(((SqlIdentifier) left).id(), ars);
        } else {
          throw new IllegalStateException(String.format("Unexpected Left paren"));
        }
      }
      default -> throw new IllegalStateException(String.format("Unexpected infix token %s", token));
    };
    return expr;
  }

  private SqlCast parseCast() {
    if (!tokens.consumeTokenType(Symbol.LEFT_PAREN)) {
      throw new IllegalStateException(String.format("Invalid start paren type ("));
    }
    var expr = parseExpr();
    if (expr == null) {
      throw new IllegalStateException("expression inside cast is missing");
    }
    var alias = (SqlAlias) expr;
    if (!tokens.consumeTokenType(Symbol.RIGHT_PAREN)) {
      throw new IllegalStateException(String.format("invalid end paren type )"));
    }
    return new SqlCast(alias.expr(), alias.alias());
  }

  private SqlSelect parseSelect() {
    var projection = parseExprList();
    if (!tokens.consumeKeyword("FROM")) {
      throw new IllegalStateException(
        String.format("Expected FROM keyword, found %s", tokens.peak()));
    }

    var table = (SqlIdentifier) parseExpr();
    // parse where
    SqlExpr filterExpr = null;
    if (tokens.consumeKeyword("WHERE")) {
      filterExpr = parseExpr();
    }

    // group by
    List<SqlExpr> groupBy = List.of();
    if (tokens.consumeKeyword(List.of("GROUP", "BY"))) {
      groupBy = parseExprList();
    }

    // having
    SqlExpr having = null;
    if (tokens.consumeKeyword("HAVING")) {
      having = parseExpr();
    }
    // order by
    List<SqlExpr> orderBy = List.of();
    if (tokens.consumeKeyword(List.of("ORDER", "BY"))) {
      orderBy.addAll(parseOrder());
    }
    return new SqlSelect(projection, filterExpr, groupBy, orderBy, having, table.id());
  }

  private SqlExpr parseExpr() {
    return parse(0);
  }

  private List<SqlExpr> parseExprList() {
    log.info("parseExprList()");
    var list = new ArrayList<SqlExpr>();
    var expr = parse();
    while (expr != null) {
      list.add(expr);
      if (tokens.peak().type() != null && tokens.peak().type().equals(Symbol.COMMA)) {
        tokens.next();
      } else {
        break;
      }
      expr = parseExpr();
    }
    return list;
  }

  private List<SqlSort> parseOrder() {
    var sortList = new ArrayList<SqlSort>();
    var sort = parseExpr();
    while (sort != null) {
      var ss = switch (sort) {
        case SqlIdentifier s -> new SqlSort(s, true);
        case SqlSort s -> s;
        default -> throw new IllegalStateException(
          String.format("Unexpected expression sort %s after order by", sort));
      };
      sortList.add(ss);
      if (tokens.peak() != null && tokens.peak().type() == Symbol.COMMA) {
        tokens.next();
      } else {
        break;
      }
      sort = parseExpr();
    }
    return sortList;
  }

  private SqlIdentifier parseIdentifier() {
    var expr = parseExpr();
    if (expr == null) {
      throw new IllegalStateException("Expected identifier, found EOF");
    }
    return switch (expr) {
      case SqlIdentifier i -> i;
      default ->
        throw new IllegalStateException(String.format("expected identifier, found %s", expr));
    };
  }
}
