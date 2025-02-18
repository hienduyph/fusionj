package dev.hienph.fusionj.sql;

import dev.hienph.fusionj.datatypes.Field;
import dev.hienph.fusionj.logical.Add;
import dev.hienph.fusionj.logical.AggregateExpr;
import dev.hienph.fusionj.logical.Alias;
import dev.hienph.fusionj.logical.And;
import dev.hienph.fusionj.logical.Avg;
import dev.hienph.fusionj.logical.BinaryExpr;
import dev.hienph.fusionj.logical.CastExpr;
import dev.hienph.fusionj.logical.Column;
import dev.hienph.fusionj.logical.ColumnIndex;
import dev.hienph.fusionj.logical.DataFrame;
import dev.hienph.fusionj.logical.Divide;
import dev.hienph.fusionj.logical.Eq;
import dev.hienph.fusionj.logical.Gt;
import dev.hienph.fusionj.logical.GtEq;
import dev.hienph.fusionj.logical.LiteralDouble;
import dev.hienph.fusionj.logical.LiteralLong;
import dev.hienph.fusionj.logical.LiteralString;
import dev.hienph.fusionj.logical.LogicalExpr;
import dev.hienph.fusionj.logical.Lt;
import dev.hienph.fusionj.logical.LtEq;
import dev.hienph.fusionj.logical.Max;
import dev.hienph.fusionj.logical.Min;
import dev.hienph.fusionj.logical.Modulus;
import dev.hienph.fusionj.logical.Multiply;
import dev.hienph.fusionj.logical.Neq;
import dev.hienph.fusionj.logical.Or;
import dev.hienph.fusionj.logical.Subtract;
import dev.hienph.fusionj.logical.Sum;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlPlanner {

  private static final Logger log = LoggerFactory.getLogger(SqlPlanner.class);

  public DataFrame createDataFrame(
    SqlSelect sqlSelect,
    Map<String, DataFrame> tables
  ) {
    var table = tables.get(sqlSelect.tableName());
    if (table == null) {
      throw new IllegalStateException(String.format("No table named: '%s'", sqlSelect.tableName()));
    }
    // translate projection sql expression into logical expressions
    var projectionExpr = sqlSelect.projection().stream().map(it -> createLogicalExpr(it, table))
      .toList();

    // build a list of columns referenced in the projection
    var columnNamesInProjection = getReferenceColumns(projectionExpr);
    var aggregateExprCount = projectionExpr.stream().filter(this::isAggregate).count();
    if (aggregateExprCount == 0 && !sqlSelect.groupBy().isEmpty()) {
      throw new IllegalStateException(
        "GROUP BY without aggregate expressions is not supported");
    }

    // does the filter expression reference anything not in the final projection ?
    var columnNamesInSelection = getColumnsReferencedBySelection(sqlSelect, table);
    var plan = table;
    if (aggregateExprCount == 0) {
      return planNonAggregateQuery(sqlSelect, plan, projectionExpr, columnNamesInSelection,
        columnNamesInProjection);
    }
    // plan aggregated
    var projection = new ArrayList<LogicalExpr>();
    var aggrExpr = new ArrayList<AggregateExpr>();
    var numGroupCols = sqlSelect.groupBy().size();
    AtomicInteger groupCount = new AtomicInteger();
    projectionExpr.forEach(expr -> {
      switch (expr) {
        case AggregateExpr e -> {
          projection.add(new ColumnIndex(numGroupCols + aggrExpr.size()));
          aggrExpr.add(e);
        }
        case Alias e -> {
          projection.add(new Alias(new ColumnIndex(numGroupCols + aggrExpr.size()), e.getAlias()));
          aggrExpr.add((AggregateExpr) e.getExpr());
        }
        default -> {
          projection.add(new ColumnIndex(groupCount.get()));
          groupCount.addAndGet(1);
        }
      }
    });
    plan = planAggregateQuery(projectionExpr, sqlSelect, columnNamesInSelection, plan, aggrExpr);
    plan = plan.project(projection);
    if (sqlSelect.having() != null) {
      plan = plan.filter(createLogicalExpr(sqlSelect.having(), plan));
    }
    return plan;
  }

  private DataFrame planNonAggregateQuery(
    SqlSelect select,
    DataFrame df,
    List<LogicalExpr> projectionExpr,
    Set<String> columnNamesInSelection,
    Set<String> columnNamesInProjection
  ) {
    var plan = df;
    if (select.selection() == null) {
      return plan.project(projectionExpr);
    }
    /* Returns a list containing all elements of the original collection without the first occurrence of the given element.*/
    var missing = (new HashSet<>(columnNamesInProjection));
    missing.removeAll(columnNamesInProjection);
    if (missing.isEmpty()) {
      plan = plan.project(projectionExpr);
      plan = plan.filter(createLogicalExpr(select.selection(), plan));
    } else {
      var n = projectionExpr.size();
      var newCOls = new ArrayList<>(projectionExpr);
      newCOls.addAll(missing.stream().map(Column::new).toList());
      plan = plan.project(newCOls);
      plan = plan.filter(createLogicalExpr(select.selection(), plan));

      // drop the columns that were added for the selection
      final var fields = plan.schema().fields();
      var expr = IntStream.range(0, n)
        .mapToObj(i -> (LogicalExpr) new Column(fields.get(i).name())).toList();
      plan = plan.project(expr);
    }
    return plan;
  }

  private DataFrame planAggregateQuery(
    List<LogicalExpr> projectionExpr,
    SqlSelect select,
    Set<String> columnNamesInSelection,
    DataFrame df,
    List<AggregateExpr> aggregateExpr
  ) {
    var plan = df;
    var projectionWithoutAggregates = projectionExpr.stream()
      .filter(it -> !(it instanceof AggregateExpr)).toList();
    if (select.selection() != null) {
      var columnNamesInProjectionWithoutAggregates = getReferenceColumns(
        projectionWithoutAggregates);
      var missing = new HashSet<>(columnNamesInSelection);
      missing.removeAll(columnNamesInProjectionWithoutAggregates);
      if (missing.isEmpty()) {
        plan = plan.project(projectionWithoutAggregates);
        plan = plan.filter(createLogicalExpr(select.selection(), plan));
      } else {
        var newCols = new ArrayList<>(projectionWithoutAggregates);
        newCols.addAll(missing.stream().map(Column::new).toList());
        plan = plan.project(newCols);
        plan = plan.filter(createLogicalExpr(select.selection(), plan));
      }
    }
    final var p = plan;
    var groupByExpr = select.groupBy().stream().map(it -> createLogicalExpr(it, p)).toList();
    return p.aggregate(groupByExpr, aggregateExpr);
  }

  private Set<String> getColumnsReferencedBySelection(SqlSelect select, DataFrame table) {
    var accumulator = new HashSet<String>();
    if (select.selection() != null) {
      var filterExpr = createLogicalExpr(select.selection(), table);
      visit(filterExpr, accumulator);
      var validColumnNames = table.schema().fields().stream().map(Field::name).collect(
        Collectors.toSet());
      accumulator.removeIf(name -> !validColumnNames.contains(name));
    }
    return accumulator;
  }

  private Set<String> getReferenceColumns(List<LogicalExpr> exprs) {
    var accumulator = new HashSet<String>();
    exprs.forEach(it -> visit(it, accumulator));
    return accumulator;
  }

  private void visit(LogicalExpr expr, Set<String> accumulator) {
    switch (expr) {
      case Column ex -> accumulator.add(ex.name());
      case Alias ex -> visit(ex.getExpr(), accumulator);
      case BinaryExpr ex -> {
        visit(ex.getLeft(), accumulator);
        visit(ex.getRight(), accumulator);
      }
      case AggregateExpr ex -> visit(ex.getExpr(), accumulator);
      default -> {
      }
    }
  }

  private LogicalExpr createLogicalExpr(SqlExpr rexpr, DataFrame input) {
    return switch (rexpr) {
      case SqlIdentifier expr -> new Column(expr.id());
      case SqlString expr -> new LiteralString(expr.value());
      case SqlLong expr -> new LiteralLong(expr.value());
      case SqlDouble expr -> new LiteralDouble(expr.value());
      case SqlBinaryExpr expr -> {
        var l = createLogicalExpr(expr.l(), input);
        var r = createLogicalExpr(expr.r(), input);
        yield switch (expr.op()) {
          case "=" -> new Eq(l, r);
          case "!=" -> new Neq(l, r);
          case ">" -> new Gt(l, r);
          case ">=" -> new GtEq(l, r);
          case "<" -> new Lt(l, r);
          case "<=" -> new LtEq(l, r);
          case "AND " -> new And(l, r);
          case "OR" -> new Or(l, r);
          case "+" -> new Add(l, r);
          case "-" -> new Subtract(l, r);
          case "*" -> new Multiply(l, r);
          case "/" -> new Divide(l, r);
          case "%" -> new Modulus(l, r);
          default ->
            throw new IllegalStateException(String.format("invalid binary op %s", expr.op()));
        };
      }
      case SqlAlias expr -> new Alias(createLogicalExpr(expr.expr(), input), expr.alias().id());
      case SqlCast expr ->
        new CastExpr(createLogicalExpr(expr.expr(), input), parseDataType(expr.dataType().id()));
      case SqlFunction expr -> switch (expr.id()) {
        case "MIN" -> new Min(createLogicalExpr(expr.args().getFirst(), input));
        case "MAX" -> new Max(createLogicalExpr(expr.args().getFirst(), input));
        case "SUM" -> new Sum(createLogicalExpr(expr.args().getFirst(), input));
        case "AVG" -> new Avg(createLogicalExpr(expr.args().getFirst(), input));
        default ->
          throw new IllegalStateException(String.format("invalid aggregate function %s", expr));
      };
      default -> throw new IllegalStateException(
        String.format("Can not create logical expression from sql expression: %s", rexpr));
    };
  }

  private Boolean isAggregate(LogicalExpr expr) {
    return switch (expr) {
      case AggregateExpr e -> true;
      case Alias e -> e.getExpr() instanceof AggregateExpr;
      default -> false;
    };
  }

  private ArrowType parseDataType(String id) {
    return switch (id) {
      case "double" -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      default -> throw new IllegalStateException(String.format("Invalid data type %s", id));
    };
  }

}
