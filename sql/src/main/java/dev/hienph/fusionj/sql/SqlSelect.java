package dev.hienph.fusionj.sql;

import java.util.List;
import javax.annotation.Nullable;

public record SqlSelect(
  List<SqlExpr> projection,
  @Nullable
  SqlExpr selection,
  List<SqlExpr> groupBy,
  List<SqlExpr> orderBy,
  @Nullable
  SqlExpr having,
  String tableName
) implements SqlRelation {}

