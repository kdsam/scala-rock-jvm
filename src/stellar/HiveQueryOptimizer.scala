package stellar

import stellar.HiveQueryUtils.{EQUAL_OPERATOR, FALSE_EXPR, TRUE_EXPR}

import scala.collection.mutable.ListBuffer
import scala.util.Try


/**
  * TODO: Documentation
  */
object HiveQueryOptimizer {

  def optimize(sql: String): Try[String] = {
    for {
      node <- Try(ParseUtils.parse(sql))
    } yield {
      parseToSql(node)
    }
  }

  def parseToSql(ast: ASTNode): String = {

    ast.getToken.getType match {
      case TOK_QUERY => {
        var qstring = "SELECT"
        // get SELECT
        for {
          child <- ast.getChildren.toList
          if child != null && child.getToken.getType == TOK_INSERT
        } {
          qstring += parseSelectExpr(child)
        }

        // get FROM
        for {
          child <- ast.getChildren.toList
          if child != null && child.getToken.getType == TOK_FROM
        } {
          qstring += parseFrom(child)
        }

        // get WHERE
        for {
          child <- ast.getChildren.toList
          if child != null && child.getToken.getType == TOK_INSERT
        } {
          for {
            l2child <- child.getChildren.toList
            if l2child != null && l2child.getToken.getType == TOK_WHERE
          } {
            qstring += parseWhereExpr(l2child)
          }
        }
        qstring
      }
      case _ => ""

    }
  }

  def parseFrom(ast: ASTNode): String = {
    var appendStr = ""
    var db = ""
    var table = ""
    var alias = ""
    for {
      child <- ast.getChildren.toList
      if child != null && child.getToken.getType == TOK_TABREF
    } {
      for {
        l2child <- child.getChildren.toList
        if l2child != null
      } {
        l2child.getToken.getType match {
          case TOK_TABNAME => {
            l2child.getChildren.toList match {
              case c1 :: c2 :: Nil => {
                db = c1.getText
                table = c2.getText
              }
              case c1 :: Nil => table = c1.getText
            }
          }
          case _ => alias = l2child.getText
        }
      }
    }
    appendStr += " FROM " + (if(db.isEmpty) "" else s"$db.") + table + (if(alias.isEmpty) "" else s" $alias")
    appendStr
  }

  def parseSelectExpr(ast: ASTNode): String = {
    var appendStr = ""
    for {
      child <- ast.getChildren.toList
      if child != null && child.getToken.getType == TOK_SELECT
    } {
      for {
        l2child <- child.getChildren.toList
        if l2child != null && l2child.getToken.getType == TOK_SELEXPR
      } {
        for {
          l3child <- l2child.getChildren.toList
          if l3child != null
        } {
          l3child.getToken.getType match {
            case TOK_ALLCOLREF => appendStr = " *"
            case _ =>
          }
        }
      }
    }
    appendStr
  }



  def parseWhereExpr(ast: ASTNode): String = {
    val rootExpr = parseWhere(ast)
    " WHERE" + getWhereStr(rootExpr)
  }

  def getWhereStr(rootExpr: Expr): String = {
    rootExpr match {
      case AndLogicalExpr(children) => {
        var andStr = ""
        var counter = 0
        for {
          child <- children.toList
          if child != null
        } {
          counter += 1
          if (counter == 1) andStr += getWhereStr(child)
          else andStr += " AND" + getWhereStr(child)
        }
        andStr
      }
      case LogicalExpr(children) => {
        var lStr = ""
        for {
          child <- children.toList
          if child != null
        } {
          lStr += getWhereStr(child)
        }
        lStr
      }
      case BinaryExpr(column, operator, constantExpression) => {
        val bStr = (if (column.alias.isEmpty) " " else s" ${column.alias.get}.") +
          s"${column.name} $operator ${constantExpression.value}"
        applyOptimization(bStr, column, operator, constantExpression)
      }
      case _ => ""
    }
  }

  def applyOptimization(bStr: String,
                        column: Column,
                        operator: String,
                        constantExpression: ConstantExpr): String = {
    var optimized = bStr
    // TODO: make optimization fields dynamic instead of hardcoding
    if(column.name.equalsIgnoreCase("member_id")) {
      val constantVal = constantExpression.value
      val partitionVal = constantVal.substring(constantVal.size - 3)
      optimized += " AND" + (if (column.alias.isEmpty) " " else s" ${column.alias.get}.") +
        s"last_digits $operator '${partitionVal}"
    }
    optimized
  }


  def parseWhere(ast: ASTNode): Expr = {
    ast.getToken.getType match {
      case TOK_WHERE => {
        val root = new LogicalExpr(ListBuffer())
        for {
          child <- ast.getChildren.toList
          if child != null
        } {
          root.children += parseWhere(child)
        }
        root
      }
      case KW_AND => {
        val andExpr = AndLogicalExpr(ListBuffer())
        for {
          child <- ast.getChildren.toList
          if child != null
        } {
          andExpr.children += parseWhere(child)
        }
        andExpr
      }
      case EQUAL => {
        var colName: String = ""
        var tableAlias: String = ""
        var value: String = ""
        for {
          child <- ast.getChildren.toList
          if child != null
        } {
          child.getToken.getType match {
            case TOK_TABLE_OR_COL => {
              colName = child.getChild(0).getText
            }
            case DOT => {
              tableAlias = child.getChild(0).getChild(0).getText
              colName = child.getChild(1).getText
            }
            case KW_TRUE => {
              value = TRUE_EXPR
            }
            case KW_FALSE => {
              value = FALSE_EXPR
            }
            case StringLiteral => {
              value = child.getText
            }
          }
        }
        val column = if (tableAlias.isEmpty) Column(colName) else Column(colName, Some(tableAlias))
        BinaryExpr(column,  EQUAL_OPERATOR, ConstantExpr(value))
      }
      case _ => new LogicalExpr(ListBuffer())
    }
  }

  def parse(ast: ASTNode): List[Expr] = {
    val buffer = ListBuffer[Expr]()

    ast.getToken.getType match {
      case TOK_QUERY =>
        for {
          child <- ast.getChildren.toList
          if child != null
        } {
          buffer ++= parse(child)
        }
      case TOK_FROM =>
        // Handles parsing for the tables
        for {
          child <- ast.getChildren.toList
          if child != null
        } {
          buffer ++= parseTable(child)
        }

      case TOK_WHERE =>
        // Handles WHERE clauses
        for {
          child <- ast.getChildren.toList
          if child != null
        } {
          buffer ++= parseWhere2(child)
        }

      case TOK_INSERT =>
        for {
          child <- ast.getChildren.toList
          if child != null
        } {
          buffer ++= parse(child)
        }
      case TOK_SELECT =>
        for {
          child <- ast.getChildren.toList
          if child != null
        } {
          buffer ++= parseSelect(child)
        }
      case _ =>
    }
    buffer.toList
  }

  def parseSelect(ast: ASTNode): List[SelectExpr] = {
    val buffer = ListBuffer[SelectExpr]()
    ast.getType match {
      case TOK_SELEXPR => for {
        child <- ast.getChildren.toList
        if child != null
      } {
        buffer ++= parseSelect(child)
      }
      case TOK_TABLE_OR_COL => for {
        child <- ast.getChildren.toList
        if child != null
      } {
        buffer += SelectField(Column(child.getText, None))
      }
      case TOK_ALLCOLREF =>
        ast.getChildren.toList match {
          case child :: Nil =>
            ast.getChildren.toList match {
              case name :: Nil => buffer += SelectAll(Some(name.getText))
              case _ =>
            }
          case _ => buffer += SelectAll()
        }
      case _ =>
    }
    buffer.toList
  }

  private def parseTable(ast: ASTNode): List[Table] = {
    val buffer = ListBuffer[Table]()

    ast.getType match {
      case TOK_FROM | TOK_JOIN =>
        // Parse JOINS recursively
        for {
          child <- ast.getChildren.toList
          if child != null
        } {
          buffer ++= parseTable(child)
        }

      case TOK_TABREF =>
        // Parse TABLE references
        ast.getChildren.toList match {
          case name :: alias :: Nil =>
            name.getChildren.toList match {
              case db :: tbName :: Nil =>
                // Handle case where table with db name and alias
                buffer += Table(tbName.getText,
                                Some(db.getText),
                                Some(alias.getText))
              case tbName :: Nil =>
                // Handle case where table with alias, without db name
                buffer += Table(tbName.getText, None, Some(alias.getText))
              case _ =>
            }
          case name :: Nil =>
            name.getChildren.toList match {
              case db :: tbName :: Nil =>
                // Handle case where table with db name, without alias
                buffer += Table(tbName.getText, Some(db.getText))
              case tbName :: Nil =>
                // Handle case where only table name is provided
                buffer += Table(tbName.getText)
              case _ =>
            }

          case _ =>
        }

      case _ =>
    }

    buffer.toList
  }

  /**
    * Handles
    * @param ast
    * @return
    */
  private def parseWhere2(ast: ASTNode): List[BinaryExpr] = {
    val buffer = ListBuffer[BinaryExpr]()

    ast.getType match {
      case EQUAL | EQUAL_NS | GREATERTHAN | GREATERTHANOREQUALTO | LESSTHAN |
          LESSTHANOREQUALTO =>
        val operator = ast.getText
        ast.getChildren.toList match {
          case child1 :: child2 :: Nil if child1.getType == DOT =>
            // Handles case where column name has alias
            child1.getChildren.toList match {
              case head :: last :: Nil if head.getType == TOK_TABLE_OR_COL =>
                head.getChildren.toList match {
                  case alias :: Nil =>
                    val column = Column(last.getText, Option(alias.getText))
                    val constant = ConstantExpr(child2.getText)
                    buffer += BinaryExpr(column, operator, constant)
                  case _ =>
                }
              case _ =>
            }

          case child1 :: child2 :: Nil if child1.getType == TOK_TABLE_OR_COL =>
            // Handles case where column name has no alias
            val columnNode = child1.getChildren.toList.head
            val column = Column(columnNode.getText)
            val constant = ConstantExpr(child2.getText)
            buffer += BinaryExpr(column, operator, constant)

          case _ =>
        }
      case KW_AND | KW_OR =>
        // Handle case where WHERE expression has multiple binary expression separated by AND/OR
        for {
          child <- ast.getChildren.toList
          if child != null
        } {
          buffer ++= parseWhere2(child)
        }
      case _ =>
    }

    buffer.toList
  }

  implicit class ArrayListConverter(val arrayList: java.util.ArrayList[Node])
      extends AnyVal {
    def toList: List[ASTNode] = {
      if (arrayList != null) {
        import scala.collection.JavaConverters._
        arrayList.asScala.map(_.asInstanceOf[ASTNode]).toList
      } else {
        List.empty[ASTNode]
      }
    }
  }

  sealed trait Expr

  class LogicalExpr(val children: ListBuffer[Expr]) extends Expr
  object LogicalExpr {
    def unapply(arg: LogicalExpr): Option[ListBuffer[Expr]] = Some(arg.children)
  }
  case class AndLogicalExpr(override val children: ListBuffer[Expr]) extends LogicalExpr(children)
  case class OrLogicalExpr(override val children: ListBuffer[Expr]) extends LogicalExpr(children)

  case class Table(name: String,
                   db: Option[String] = None,
                   alias: Option[String] = None)
      extends Expr

  case class Column(name: String, alias: Option[String] = None)

  case class BinaryExpr(column: Column,
                        operator: String,
                        constantExpression: ConstantExpr)
      extends Expr

  sealed trait SelectExpr extends Expr

  case class SelectAll(db: Option[String] = None) extends SelectExpr
  case class SelectField(column: Column) extends SelectExpr

  // TODO: Do typing on this, right now this is all String
  case class ConstantExpr(value: String) extends Expr

  case class HiveQuery(original: String, suggested: String)
}
