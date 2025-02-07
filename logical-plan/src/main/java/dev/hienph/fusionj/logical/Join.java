package dev.hienph.fusionj.logical;

import dev.hienph.fusionj.datatypes.Schema;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

public record Join(
    LogicalPlan left,
    LogicalPlan right,
    JoinType joinType,
    List<Pair<String, String>> on
) implements LogicalPlan {

  @Override
  public Schema schema() {
    var duplicateKeys = on.stream().filter(it -> it.first().equals(it.second())).map(Pair::first)
        .collect(
            Collectors.toSet());

    switch (joinType) {
      case JoinType.Inner, JoinType.Left -> {
        var fields = left.schema().fields();
        var rightFields = right.schema().fields().stream()
            .filter(it -> !duplicateKeys.contains(it.name()));
        fields.addAll(rightFields.toList());
        return new Schema(fields);
      }
      case JoinType.Right -> {
        var fields = left.schema().fields().stream()
            .filter(it -> !duplicateKeys.contains(it.name())).collect(
                Collectors.toList());
        fields.addAll(right.schema().fields());
        return new Schema(fields);
      }

      default -> throw new RuntimeException(new SQLException("invalid join type"));
    }
  }

  @Override
  public List<LogicalPlan> children() {
    return List.of(left, right);
  }
}
