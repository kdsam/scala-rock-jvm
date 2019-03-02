package stellar

case class MemberPartitionWhereOptimizeDefinition() extends WhereOptimizeDefinition {

  val columnName = "member_id"

  override def ifPresent(expr: Expr): Boolean = {
    expr match {
      case BinaryExpr(leftVal, operator, rightVal) =>
        operator match {
          case "=" =>
            rightVal match {
              case StringConstantExpr(_) =>
                leftVal match {
                  case Column(name, _) =>
                    name match {
                      case `columnName` => true
                      case _ => false
                    }
                  case _ => false
                }
              case _ => false
            }
          case _ => false
        }
      case _ => false
    }
  }
  override def applyOptimization(expr: Expr): Expr = {
    val binaryExpr = expr.asInstanceOf[BinaryExpr]
    val inputColumn = binaryExpr.leftVal.asInstanceOf[Column]
    val strExpr = binaryExpr.rightVal.asInstanceOf[StringConstantExpr]
    val memberId = strExpr.value
    val partitionVal = getPartitionValue(memberId)
    val partitionCol = Column("member_hash", inputColumn.alias)
    BinaryExpr(partitionCol, "=", StringConstantExpr(partitionVal))
  }

  private def getPartitionValue(memberId: String): String = {
    val md5StrLast32Bits = DigestUtils.md5Hex(memberId).substring(24)
    val base10modulo10 = Integer.parseUnsignedInt(md5StrLast32Bits, 16) % 10
    base10modulo10.toString
  }
}
