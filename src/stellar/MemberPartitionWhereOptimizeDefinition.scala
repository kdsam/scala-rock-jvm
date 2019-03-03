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
    val strVal = strExpr.value
    val memberId = sanitize(strVal)
    val partitionVal = getPartitionValue(memberId)
    val partitionCol = Column("member_hash", inputColumn.alias)
    BinaryExpr(partitionCol, "=", StringConstantExpr(partitionVal))
  }

  private def getPartitionValue(memberId: String): String = {
    val md5StrLast32Bits = DigestUtils.md5Hex(memberId).substring(24)
    val base10modulo10 = Integer.parseUnsignedInt(md5StrLast32Bits, 16) % 10
    s"'${base10modulo10.toString}'"
  }

  private def sanitize(input: String): String = {
    var sanitized = input.trim
    if (sanitized.startsWith("'") || sanitized.startsWith("\""))
      sanitized = sanitized.substring(1)
    if (sanitized.endsWith("'") || sanitized.endsWith("\""))
      sanitized = sanitized.substring(0, sanitized.length - 1)
    sanitized
  }
}

object test extends App {
  println(getPartitionValue("'M-000000001'"))


  private def getPartitionValue(memberId: String): String = {
    val md5StrLast32Bits = DigestUtils.md5Hex(memberId).substring(24)
    val base10modulo10 = Integer.parseUnsignedInt(md5StrLast32Bits, 16) % 10
    s"'${base10modulo10.toString}'"
  }
}


