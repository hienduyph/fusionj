package dev.hienph.fusionj.sql;

public  record SqlAlias( SqlExpr expr, SqlIdentifier alias) implements SqlExpr {

}
