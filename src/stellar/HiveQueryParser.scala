package stellar

import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
  * TODO: Documentation
  */
object HiveQueryParser {

  // TODO: Separate List of Tables and List of Binary Expressions instead of returning generic Expressions
  // TODO: convert to Nodes to be compatible with subqueries
  def optimize(sql: String): Try[List[Expr]] = {
    for {
      node <- Try(ParseUtils.parse(sql))
    } yield {
      parse(node)
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
          buffer ++= parseWhere(child)
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
  private def parseWhere(ast: ASTNode): List[BinaryExpr] = {
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
          buffer ++= parseWhere(child)
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
}
