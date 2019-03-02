package stellar

sealed trait OptimizeDefinition {
  def ifPresent(expr: Expr): Boolean
  def applyOptimization(expr: Expr): Expr
}

abstract class WhereOptimizeDefinition extends OptimizeDefinition
