package dev.hienph.fusionj.planner;

import dev.hienph.fusionj.datatypes.Schema;
import dev.hienph.fusionj.logical.Add;
import dev.hienph.fusionj.logical.Aggregate;
import dev.hienph.fusionj.logical.Alias;
import dev.hienph.fusionj.logical.And;
import dev.hienph.fusionj.logical.BinaryExpr;
import dev.hienph.fusionj.logical.CastExpr;
import dev.hienph.fusionj.logical.Column;
import dev.hienph.fusionj.logical.ColumnIndex;
import dev.hienph.fusionj.logical.Divide;
import dev.hienph.fusionj.logical.Eq;
import dev.hienph.fusionj.logical.Gt;
import dev.hienph.fusionj.logical.GtEq;
import dev.hienph.fusionj.logical.LiteralDouble;
import dev.hienph.fusionj.logical.LiteralLong;
import dev.hienph.fusionj.logical.LiteralString;
import dev.hienph.fusionj.logical.LogicalExpr;
import dev.hienph.fusionj.logical.LogicalPlan;
import dev.hienph.fusionj.logical.Max;
import dev.hienph.fusionj.logical.Min;
import dev.hienph.fusionj.logical.Multiply;
import dev.hienph.fusionj.logical.Neq;
import dev.hienph.fusionj.logical.Or;
import dev.hienph.fusionj.logical.Projection;
import dev.hienph.fusionj.logical.Scan;
import dev.hienph.fusionj.logical.Selection;
import dev.hienph.fusionj.logical.Subtract;
import dev.hienph.fusionj.logical.Sum;
import dev.hienph.fusionj.physical.HashAggregateExec;
import dev.hienph.fusionj.physical.PhysicalPlan;
import dev.hienph.fusionj.physical.ProjectionExec;
import dev.hienph.fusionj.physical.ScanExec;
import dev.hienph.fusionj.physical.SelectionExec;
import dev.hienph.fusionj.physical.expresisons.AddExpression;
import dev.hienph.fusionj.physical.expresisons.AggregateExpression;
import dev.hienph.fusionj.physical.expresisons.AndExpression;
import dev.hienph.fusionj.physical.expresisons.CastExpression;
import dev.hienph.fusionj.physical.expresisons.ColumnExpression;
import dev.hienph.fusionj.physical.expresisons.DivideExpression;
import dev.hienph.fusionj.physical.expresisons.EqExpression;
import dev.hienph.fusionj.physical.expresisons.Expression;
import dev.hienph.fusionj.physical.expresisons.GtEqExpression;
import dev.hienph.fusionj.physical.expresisons.GtExpression;
import dev.hienph.fusionj.physical.expresisons.LiteralDoubleExpression;
import dev.hienph.fusionj.physical.expresisons.LiteralLongExpression;
import dev.hienph.fusionj.physical.expresisons.LiteralStringExpression;
import dev.hienph.fusionj.physical.expresisons.MaxExpression;
import dev.hienph.fusionj.physical.expresisons.MinExpression;
import dev.hienph.fusionj.physical.expresisons.MultiplyExpression;
import dev.hienph.fusionj.physical.expresisons.NeqExpression;
import dev.hienph.fusionj.physical.expresisons.OrExpression;
import dev.hienph.fusionj.physical.expresisons.SubtractExpression;
import dev.hienph.fusionj.physical.expresisons.SumExpression;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class QueryPlanner {

  public PhysicalPlan createPhysicalPlan(LogicalPlan oplan) {

    return switch (oplan) {
      case Scan sc -> new ScanExec(sc.dataSource(), sc.projection());
      case Selection s -> {
        var input = createPhysicalPlan(s.input());
        var filterExpr = createPhysicalPlanExpr(s.expr(), s.input());
        yield new SelectionExec(input, filterExpr);
      }
      case Projection plan -> {
        var input = createPhysicalPlan(plan.input());
        var projectionExpr = plan.expr().stream()
            .map(it -> createPhysicalPlanExpr(it, plan.input())).toList();
        var projectionSchema = new Schema(
            plan.expr().stream().map(it -> it.toField(plan.input())).toList());
        yield new ProjectionExec(input, projectionSchema, projectionExpr);
      }
      case Aggregate plan -> {
        var input = createPhysicalPlan(plan.input());
        var groupExpr = plan.groupExpr().stream()
            .map(it -> createPhysicalPlanExpr(it, plan.input())).collect(
                Collectors.toList());
        var aggregateExpr = plan.aggregateExpr().stream()
            .map(it -> (AggregateExpression) switch (it) {
              case Min m -> new MinExpression(createPhysicalPlanExpr(m.getExpr(), plan.input()));
              case Max m -> new MaxExpression(createPhysicalPlanExpr(m.getExpr(), plan.input()));
              case Sum m -> new SumExpression(createPhysicalPlanExpr(m.getExpr(), plan.input()));
              default -> throw new IllegalStateException(
                  String.format("Unsupported aggregate function: %s", it));
            }).toList();
        yield new HashAggregateExec(input, groupExpr, aggregateExpr, plan.schema());
      }
      default -> throw new IllegalStateException(oplan.getClass().toString());
    };
  }

  public Expression createPhysicalPlanExpr(LogicalExpr oexpr, LogicalPlan input) {
    return switch (oexpr) {
      case LiteralLong expr -> new LiteralLongExpression(expr.val());
      case LiteralDouble expr -> new LiteralDoubleExpression(expr.val());
      case LiteralString expr -> new LiteralStringExpression(expr.str());
      case ColumnIndex expr -> new ColumnExpression(expr.i());
      case Alias expr -> createPhysicalPlanExpr(expr.getExpr(), input);
      case Column expr -> {
        var i = IntStream.range(0, input.schema().fields().size())
            .filter(it -> input.schema().fields().get(it).name().equals(expr.name())).findFirst();
        if (i.isEmpty()) {
          throw new IllegalArgumentException(String.format("No column named %s", expr.name()));
        }
        yield new ColumnExpression(i.getAsInt());
      }
      case CastExpr expr ->
          new CastExpression(createPhysicalPlanExpr(expr.expr(), input), expr.dataType());
      case BinaryExpr expr -> {
        var l = createPhysicalPlanExpr(expr.getLeft(), input);
        var r = createPhysicalPlanExpr(expr.getRight(), input);
        yield switch (expr) {
          case Eq e -> new EqExpression(l, r);
          case Neq e -> new NeqExpression(l, r);
          case Gt e -> new GtExpression(l, r);
          case GtEq e -> new GtEqExpression(l, r);
          case And e -> new AndExpression(l, r);
          case Or e -> new OrExpression(l, r);
          case Add e -> new AddExpression(l, r);
          case Subtract e -> new SubtractExpression(l, r);
          case Multiply e -> new MultiplyExpression(l, r);
          case Divide e -> new DivideExpression(l, r);
          default ->
              throw new IllegalStateException(String.format("Unsuport binary expression %s", expr));
        };
      }
      default -> throw new IllegalStateException(
          String.format("Unsupported logical expression %s", oexpr));
    };
  }
}
