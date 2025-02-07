package dev.hienph.fusionj.logical;

public class Modulus extends MathExpr {

  public Modulus(LogicalExpr left, LogicalExpr right) {
    super("mod", "%s", left, right);
  }

}
