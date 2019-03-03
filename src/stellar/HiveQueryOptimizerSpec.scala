package stellar

/**
  * <p>Test cases for Parsing Hive queries
  * Divided into these sections:
  * </p>
  * <ul>
  *   <li>#1 SIMPLE QUERIES TEST CASES</li>
  *   <li>#2 SELECT TEST CASES</li>
  *   <li>#3 SELECT FIELDS TEST CASES</li>
  *   <li>#4 WHERE CLAUSES TEST CASES</li>
  *   <li>#5 GREATER THAN LESS THAN OPERATORS</li>
  *   <li>#6 OR, AND and NOT LOGICAL OPERATORS W/ PARENTHESIS/GROUPING</li>
  *   <li>#7 ORDER BY TEST CASES</li>
  *   <li>#8 IS NULL TEST CASES</li>
  *   <li>#9 SELECT With LIMIT TEST CASES</li>
  *   <li>#10 MIN() and MAX() TEST CASES</li>
  *   <li>#11 COUNT, AVG, SUM TEST CASES</li>
  *   <li>#12 SQL LIKE TEST CASES</li>
  *   <li>#13 SQL WILDCARDS TEST CASES</li>
  *   <li>#14 SQL IN OPERATOR TEST CASES</li>
  *   <li>#15 SQL BETWEEN TEST CASES</li>
  *   <li>#16 SQL ALIASES TEST CASES</li>
  *   <li>#17 JOIN TEST CASES</li>
  *   <li>#18 UNION TEST CASES</li>
  *   <li>#19 GROUP BY TEST CASES</li>
  *   <li>#20 HAVING TEST CASES</li>
  *   <li>#21 SQL EXISTS TEST CASES</li>
  *   <li>#22 SELECT CASE TEST CASES</li>
  *   <li>#23 SQL IFNULL, ISNULL, COALESCE, and NVL Functions TEST CASES</li>
  *   <li>#24 SUBQUERIES TEST CASES</li>
  * </ul>
  */
class HiveQueryOptimizerSpec extends FlatSpec with Matchers with Inside {
  behavior of "HiveQueryOptimizer"

  val memberPartitionOptimizer = MemberPartitionWhereOptimizeDefinition()

  /*********************************** #1 SIMPLE QUERIES TEST CASES ********************************************/

  it should "parse simple queries" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001' AND test = true
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001'
                               | AND test = true
           """.stripMargin.replaceAll("\n", "").trim)
  }

  it should "optimize simple queries" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001' AND test = true
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql, memberPartitionOptimizer)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001'
                               | AND M.member_hash = '9'
                               | AND test = true
           """.stripMargin.replaceAll("\n", "").trim)
  }

  it should "optimize simple queries2" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
                               | AND M.last_digits = '01'
           """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize simple queries without table alias" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member WHERE test = true AND member_id = 'M-000000001'
       """.stripMargin
    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM growingtree.member WHERE test = true AND member_id = 'M-000000001'
                               | AND last_digits = '01'
           """.stripMargin.replaceAll("\n", "").trim)

  }

  /*********************************** #2 SELECT TEST CASES ********************************************/

  it should "optimize simple SELECT" in {
    val sql =
      s"""
         |SELECT CustomerName, City FROM Customers
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT CustomerName, City FROM Customers
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize SELECT DISTINCT queries" in {
    val sql =
      s"""
         |SELECT DISTINCT Country FROM Customers
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT DISTINCT Country FROM Customers
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize SELECT COUNT DISTINCT queries" in {
    val sql =
      s"""
         |SELECT COUNT(DISTINCT Country) FROM Customers
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT COUNT(DISTINCT Country) FROM Customers
       """.stripMargin.replaceAll("\n", "").trim)

  }

  /*********************************** #3 SELECT FIELDS TEST CASES **************************************************/

  it should "optimize simple queries with single select field" in {
    val sql =
      s"""
         |SELECT member_id FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT member_id FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
                               | AND M.last_digits = '01'
           """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize simple queries with multiple select field" in {
    val sql =
      s"""
         |SELECT member_id, first_name, email FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT member_id, first_name, email FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
                               | AND M.last_digits = '01'
           """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize simple queries with multiple select field combined with select *" in {
    val sql =
      s"""
         |SELECT *, member_id, first_name, email FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
       """.stripMargin
    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT *, member_id, first_name, email FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
                               | AND M.last_digits = '01'
           """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize simple queries with multiple select field combined with select * with table alias" in {
    val sql =
      s"""
         |SELECT m.*, m.member_id, m.first_name, m.email FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT m.*, m.member_id, m.first_name, m.email FROM growingtree.member M WHERE test = true AND
                               | M.member_id = 'M-000000001'
                               | AND M.last_digits = '01'
           """.stripMargin.replaceAll("\n", "").trim)

  }

  /*********************************** #4 WHERE CLAUSES TEST CASES **************************************************/

  it should "optimize simple WHERE clause" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | WHERE Country='Mexico'
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | WHERE Country = 'Mexico'
           """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize simple WHERE clause with Numeric Field" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | WHERE CustomerID = 1
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | WHERE CustomerID = 1
       """.stripMargin.replaceAll("\n", "").trim)

  }

  /*********************************** #5 GREATER THAN LESS THAN OPERATORS *******************************************/

  it should "optimize simple queries with greater than operator" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001' AND M.age_yrs > 18
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001'
                               | AND M.last_digits = '01' AND M.age_yrs > 18
           """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize simple queries with less than operator" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001' AND M.age_yrs < 18
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001'
                               | AND M.last_digits = '01' AND M.age_yrs < 18
           """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize simple queries with less than or equal operator" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001' AND M.age_yrs <= 18
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001'
                               | AND M.last_digits = '01' AND M.age_yrs <= 18
           """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize simple queries with greater than or equal operator" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001' AND M.age_yrs >= 18
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001'
                               | AND M.last_digits = '01' AND M.age_yrs >= 18
           """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize simple queries with greater than operator in WHERE clause" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE age(birthdate) > 2
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM growingtree.member M WHERE age(birthdate) > 2
           """.stripMargin.replaceAll("\n", "").trim)

  }

  /*********************** #6 OR, AND and NOT LOGICAL OPERATORS W/ PARENTHESIS/GROUPING *****************************/

  it should "optimize simple queries with OR operator" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001' AND M.age_yrs >= 18 or test = true
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM growingtree.member M WHERE (M.member_id = 'M-000000001'
                               | AND M.last_digits = '01' AND M.age_yrs >= 18) OR test = true
           """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize simple queries with OR operator and AND enclosed in parenthesis" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE (M.member_id = 'M-000000001' AND M.age_yrs >= 18) or test = true
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM growingtree.member M WHERE (M.member_id = 'M-000000001'
                               | AND M.last_digits = '01' AND M.age_yrs >= 18) OR test = true
           """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize simple queries with AND operator and OR enclosed in parenthesis" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001' AND (M.age_yrs >= 18 or test = true)
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001'
                               | AND M.last_digits = '01' AND (M.age_yrs >= 18 OR test = true)
           """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize simple queries with AND operator and OR proper grouping" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE (M.member_id = 'M-000000001' AND M.test_field = 1) OR
         | (M.age_yrs >= 18 AND test = true)
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM growingtree.member M WHERE (M.member_id = 'M-000000001' AND M.last_digits = '01'
                               | AND M.test_field = 1) OR (M.age_yrs >= 18 AND test = true)
           """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize simple AND query" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | WHERE Country='Germany' AND City='Berlin'
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                                |SELECT * FROM Customers
                                | WHERE Country = 'Germany' AND City = 'Berlin'
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize simple OR query" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | WHERE City='Berlin' OR City='München'
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | WHERE City = 'Berlin' OR City = 'München'
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize simple NOT query" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | WHERE NOT Country='Germany'
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | WHERE NOT Country = 'Germany'
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize combined AND and OR query" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | WHERE Country='Germany' AND (City='Berlin' OR City='München')
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | WHERE Country = 'Germany' AND (City = 'Berlin' OR City = 'München')
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize combined AND and NOT query" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | WHERE NOT Country='Germany' AND NOT Country='USA'
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | WHERE NOT Country = 'Germany' AND NOT Country = 'USA'
       """.stripMargin.replaceAll("\n", "").trim)

  }

  /*********************************** #7 ORDER BY TEST CASES ******************************************************/

  it should "optimize queries with order by" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001' order by last_name
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
                               | AND M.last_digits = '01' ORDER BY last_name ASC
           """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with order by asc" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001' order by last_name asc
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
                               | AND M.last_digits = '01' ORDER BY last_name ASC
           """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with order by desc" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001' order by last_name desc
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
                               | AND M.last_digits = '01' ORDER BY last_name DESC
           """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with order by of multiple columns" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001' order by last_name, first_name
       """.stripMargin
    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
                               | AND M.last_digits = '01' ORDER BY last_name ASC, first_name ASC
           """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with order by of multiple columns different sort per column" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001' order by last_name asc,
         | first_name desc
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
                               | AND M.last_digits = '01' ORDER BY last_name ASC, first_name DESC
           """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with order by with table alias" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001' order by m.last_name asc,
         | m.first_name desc
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
                               | AND M.last_digits = '01' ORDER BY m.last_name ASC, m.first_name DESC
           """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize simple ORDER BY queries" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | ORDER BY Country
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | ORDER BY Country ASC
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize simple ORDER BY DESC queries" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | ORDER BY Country DESC
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | ORDER BY Country DESC
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize ORDER BY Several Columns queries" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | ORDER BY Country, CustomerName
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | ORDER BY Country ASC, CustomerName ASC
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize ORDER BY Several Columns ASC and DESC queries" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | ORDER BY Country ASC, CustomerName DESC
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | ORDER BY Country ASC, CustomerName DESC
       """.stripMargin.replaceAll("\n", "").trim)

  }

  /*************************************** #8 IS NULL TEST CASES **********************************************/

  it should "optimize IS NULL queries" in {
    val sql =
      s"""
         |SELECT CustomerName, ContactName, Address
         | FROM Customers
         | WHERE Address IS NULL
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT CustomerName, ContactName, Address
                               | FROM Customers
                               | WHERE Address IS NULL
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize IS NOT NULL queries" in {
    val sql =
      s"""
         |SELECT CustomerName, ContactName, Address
         | FROM Customers
         | WHERE Address IS NOT NULL
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT CustomerName, ContactName, Address
                               | FROM Customers
                               | WHERE Address IS NOT NULL
       """.stripMargin.replaceAll("\n", "").trim)

  }

  /*************************************** #9 SELECT With LIMIT TEST CASES **********************************************/

  it should "optimize simple SELECT with LIMIT queries" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | LIMIT 3
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | LIMIT 3
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize SELECT with LIMIT using ROWNUM queries" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | WHERE ROWNUM <= 3
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | WHERE ROWNUM <= 3
       """.stripMargin.replaceAll("\n", "").trim)

  }

  /********************************* #10 MIN() and MAX() TEST CASES **********************************************/

  it should "optimize simple MIN() queries" in {
    val sql =
      s"""
         |SELECT MIN(Price) AS SmallestPrice
         | FROM Products
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT MIN(Price) AS SmallestPrice
                               | FROM Products
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize simple MAX() queries" in {
    val sql =
      s"""
         |SELECT MAX(Price) AS LargestPrice
         | FROM Products
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT MAX(Price) AS LargestPrice
                               | FROM Products
       """.stripMargin.replaceAll("\n", "").trim)

  }

  /********************************* #11 COUNT, AVG, SUM TEST CASES **********************************************/

  it should "optimize simple COUNT() queries" in {
    val sql =
      s"""
         |SELECT COUNT(ProductID)
         | FROM Products
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT COUNT(ProductID)
                               | FROM Products
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize simple AVG() queries" in {
    val sql =
      s"""
         |SELECT AVG(Price)
         | FROM Products
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT AVG(Price)
                               | FROM Products
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize simple SUM() queries" in {
    val sql =
      s"""
         |SELECT SUM(Quantity)
         | FROM OrderDetails
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT SUM(Quantity)
                               | FROM OrderDetails
       """.stripMargin.replaceAll("\n", "").trim)

  }

  /********************************* #12 SQL LIKE TEST CASES **********************************************/

  it should "optimize simple LIKE queries" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | WHERE CustomerName LIKE 'a%'
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                                |SELECT * FROM Customers
                                | WHERE CustomerName LIKE 'a%'
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize simple LIKE matching ending character" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | WHERE CustomerName LIKE '%a'
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | WHERE CustomerName LIKE '%a'
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize simple LIKE matching 'or' in any position" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | WHERE CustomerName LIKE '%or%'
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | WHERE CustomerName LIKE '%or%'
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize LIKE matching character in second position" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | WHERE CustomerName LIKE '_r%'
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | WHERE CustomerName LIKE '_r%'
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize LIKE with starting character and at least 3 characters length" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | WHERE CustomerName LIKE 'a_%_%'
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | WHERE CustomerName LIKE 'a_%_%'
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize LIKE with starting character and ending character" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | WHERE ContactName LIKE 'a%o'
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | WHERE ContactName LIKE 'a%o'
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize LIKE does not start with a character" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | WHERE CustomerName NOT LIKE 'a%'
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | WHERE NOT CustomerName LIKE 'a%'
       """.stripMargin.replaceAll("\n", "").trim)

  }

  /********************************* #13 SQL WILDCARDS TEST CASES **********************************************/

  it should "optimize queries with trailing % WILDCARD" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | WHERE City LIKE 'ber%'
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | WHERE City LIKE 'ber%'
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with leading and trailing % WILDCARD" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | WHERE City LIKE '%es%'
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | WHERE City LIKE '%es%'
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with _ WILDCARD" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | WHERE City LIKE '_erlin'
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | WHERE City LIKE '_erlin'
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with [charlist] WILDCARD" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | WHERE City LIKE '[bsp]%'
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | WHERE City LIKE '[bsp]%'
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with [<range character list>] WILDCARD" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | WHERE City LIKE '[a-c]%'
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | WHERE City LIKE '[a-c]%'
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with [!charlist] WILDCARD" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | WHERE City LIKE '[!bsp]%'
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | WHERE City LIKE '[!bsp]%'
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with NOT [charlist] WILDCARD" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | WHERE City NOT LIKE '[bsp]%'
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | WHERE NOT City LIKE '[bsp]%'
       """.stripMargin.replaceAll("\n", "").trim)

  }

  /********************************* #14 SQL IN OPERATOR TEST CASES **********************************************/

  it should "optimize simple IN queries" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | WHERE Country IN ('Germany', 'France', 'UK')
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | WHERE Country IN ('Germany', 'France', 'UK')
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize simple NOT IN queries" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | WHERE Country NOT IN ('Germany', 'France', 'UK')
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | WHERE NOT Country IN ('Germany', 'France', 'UK')
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize IN subqueries" in {
    val sql =
      s"""
         |SELECT * FROM Customers
         | WHERE Country IN (SELECT Country FROM Suppliers)
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Customers
                               | WHERE Country IN (SELECT Country FROM Suppliers)
       """.stripMargin.replaceAll("\n", "").trim)

  }

  /********************************* #15 SQL BETWEEN TEST CASES **********************************************/

  it should "optimize simple BETWEEN queries" in {
    val sql =
      s"""
         |SELECT * FROM Products
         | WHERE Price BETWEEN 10 AND 20
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Products
                               | WHERE Price BETWEEN 10 AND 20
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize NOT BETWEEN queries" in {
    val sql =
      s"""
         |SELECT * FROM Products
         | WHERE Price NOT BETWEEN 10 AND 20
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Products
                               | WHERE NOT Price BETWEEN 10 AND 20
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize BETWEEN with IN queries" in {
    val sql =
      s"""
         |SELECT * FROM Products
         | WHERE (Price BETWEEN 10 AND 20)
         | AND NOT CategoryID IN (1,2,3)
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Products
                               | WHERE (Price BETWEEN 10 AND 20)
                               | AND NOT CategoryID IN (1, 2, 3)
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize BETWEEN Text Values queries" in {
    val sql =
      s"""
         |SELECT * FROM Products
         | WHERE ProductName BETWEEN 'Carnarvon Tigers' AND 'Mozzarella di Giovanni'
         | ORDER BY ProductName
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Products
                               | WHERE ProductName BETWEEN 'Carnarvon Tigers' AND 'Mozzarella di Giovanni'
                               | ORDER BY ProductName ASC
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize NOT BETWEEN Text Values queries" in {
    val sql =
      s"""
         |SELECT * FROM Products
         | WHERE ProductName NOT BETWEEN 'Carnarvon Tigers' AND 'Mozzarella di Giovanni'
         | ORDER BY ProductName
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Products
                               | WHERE NOT ProductName BETWEEN 'Carnarvon Tigers' AND 'Mozzarella di Giovanni'
                               | ORDER BY ProductName ASC
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize BETWEEN Dates queries" in {
    val sql =
      s"""
         |SELECT * FROM Orders
         | WHERE OrderDate BETWEEN '1996-07-01' AND '1996-07-31'
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM Orders
                               | WHERE OrderDate BETWEEN '1996-07-01' AND '1996-07-31'
       """.stripMargin.replaceAll("\n", "").trim)

  }

  /********************************* #16 SQL ALIASES TEST CASES **********************************************/

  it should "optimize simple queries with Aliases" in {
    val sql =
      s"""
         |SELECT CustomerID AS ID, CustomerName AS Customer
         | FROM Customers
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT CustomerID AS ID, CustomerName AS Customer
                               | FROM Customers
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with Aliases containing spaces" in {
    val sql =
      s"""
         |SELECT CustomerName AS Customer, ContactName AS `Contact person`
         | FROM Customers
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT CustomerName AS Customer, ContactName AS `Contact person`
                               | FROM Customers
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with Alias from combined columns" in {
    val sql =
      s"""
         |SELECT CustomerName, Address + ', ' + PostalCode + ' ' + City + ', ' + Country AS Address
         | FROM Customers
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT CustomerName, Address + ', ' + PostalCode + ' ' + City + ', ' + Country AS Address
                               | FROM Customers
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with Alias from concatenated columns" in {
    val sql =
      s"""
         |SELECT CustomerName, CONCAT(Address,', ',PostalCode,', ',City,', ',Country) AS Address
         | FROM Customers
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT CustomerName, CONCAT(Address, ', ', PostalCode, ', ', City, ', ', Country)
                               | AS Address
                               | FROM Customers
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with Alias for tables" in {
    val sql =
      s"""
         |SELECT o.OrderID, o.OrderDate, c.CustomerName
         | FROM Customers AS c, Orders AS o
         | WHERE c.CustomerName="Around the Horn" AND c.CustomerID=o.CustomerID
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT o.OrderID, o.OrderDate, c.CustomerName
                               | FROM Customers c INNER JOIN Orders o
                               | WHERE c.CustomerName = "Around the Horn" AND c.CustomerID = o.CustomerID
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries without Alias for tables" in {
    val sql =
      s"""
         |SELECT Orders.OrderID, Orders.OrderDate, Customers.CustomerName
         | FROM Customers, Orders
         | WHERE Customers.CustomerName= "Around the Horn" AND Customers.CustomerID = Orders.CustomerID
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT Orders.OrderID, Orders.OrderDate, Customers.CustomerName
                               | FROM Customers INNER JOIN Orders
                               | WHERE Customers.CustomerName = "Around the Horn" AND
                               | Customers.CustomerID = Orders.CustomerID
       """.stripMargin.replaceAll("\n", "").trim)

  }

  /*********************************** #17 JOIN TEST CASES ******************************************************/

  it should "optimize join queries" in {
    val sql =
      s"""
         |SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate
         | FROM Orders
         | INNER JOIN Customers ON Orders.CustomerID = Customers.CustomerID
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate
                               | FROM Orders
                               | INNER JOIN Customers ON Orders.CustomerID = Customers.CustomerID
       """.stripMargin.replaceAll("\n", "").trim)

  }


  it should "optimize inner join three tables" in {
    val sql =
      s"""
         |SELECT Orders.OrderID, Customers.CustomerName, Shippers.ShipperName
         | FROM
         | (Orders
         | INNER JOIN Customers ON Orders.CustomerID = Customers.CustomerID)
         | INNER JOIN Shippers ON Orders.ShipperID = Shippers.ShipperID
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT Orders.OrderID, Customers.CustomerName, Shippers.ShipperName
                               | FROM
                               | (Orders
                               | INNER JOIN Customers ON Orders.CustomerID = Customers.CustomerID)
                               | INNER JOIN Shippers ON Orders.ShipperID = Shippers.ShipperID
       """.stripMargin.replaceAll("\n", "").trim)
  }

  it should "optimize left join with order by" in {
    val sql =
      s"""
         |SELECT Customers.CustomerName, Orders.OrderID
         | FROM Customers
         | LEFT JOIN Orders ON Customers.CustomerID = Orders.CustomerID
         | ORDER BY Customers.CustomerName
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT Customers.CustomerName, Orders.OrderID
                               | FROM Customers
                               | LEFT JOIN Orders ON Customers.CustomerID = Orders.CustomerID
                               | ORDER BY Customers.CustomerName ASC
       """.stripMargin.replaceAll("\n", "").trim)
  }

  it should "optimize right join" in {
    val sql =
      s"""
         | SELECT Orders.OrderID, Employees.LastName, Employees.FirstName
         | FROM Orders
         | RIGHT JOIN Employees ON Orders.EmployeeID = Employees.EmployeeID
         | ORDER BY Orders.OrderID
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               | SELECT Orders.OrderID, Employees.LastName, Employees.FirstName
                               | FROM Orders
                               | RIGHT JOIN Employees ON Orders.EmployeeID = Employees.EmployeeID
                               | ORDER BY Orders.OrderID ASC
       """.stripMargin.replaceAll("\n", "").trim)
  }

  it should "optimize full outer join" in {
    val sql =
      s"""
         | SELECT Customers.CustomerName, Orders.OrderID
         | FROM Customers
         | FULL OUTER JOIN Orders ON Customers.CustomerID=Orders.CustomerID
         | ORDER BY Customers.CustomerName
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               | SELECT Customers.CustomerName, Orders.OrderID
                               | FROM Customers
                               | FULL OUTER JOIN Orders ON Customers.CustomerID = Orders.CustomerID
                               | ORDER BY Customers.CustomerName ASC
       """.stripMargin.replaceAll("\n", "").trim)
  }

  it should "optimize self join" in {
    val sql =
      s"""
         | SELECT A.CustomerName AS CustomerName1, B.CustomerName AS CustomerName2, A.City
         | FROM Customers A, Customers B
         | WHERE A.CustomerID <> B.CustomerID
         | AND A.City = B.City
         | ORDER BY A.City
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               | SELECT A.CustomerName AS CustomerName1, B.CustomerName AS CustomerName2, A.City
                               | FROM Customers A INNER JOIN Customers B
                               | WHERE A.CustomerID <> B.CustomerID
                               | AND A.City = B.City
                               | ORDER BY A.City ASC
       """.stripMargin.replaceAll("\n", "").trim)
  }

  /*********************************** #18 UNION TEST CASES ******************************************************/

  it should "optimize UNION queries " in {
    val sql =
      s"""
         | SELECT City FROM Customers
         | UNION
         | SELECT City FROM Suppliers
         | ORDER BY City
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               | SELECT _u2.City FROM
                               | (SELECT DISTINCT _u1.City FROM
                               | (SELECT City FROM Customers UNION ALL SELECT City FROM Suppliers) _u1) _u2
                               | ORDER BY City ASC
       """.stripMargin.replaceAll("\n", "").trim)
  }

  it should "optimize UNION ALL queries " in {
    val sql =
      s"""
         | SELECT City FROM Customers
         | UNION ALL
         | SELECT City FROM Suppliers
         | ORDER BY City
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               | SELECT _u2.City FROM
                               | (SELECT _u1.City FROM
                               | (SELECT City FROM Customers UNION ALL SELECT City FROM Suppliers) _u1) _u2
                               | ORDER BY City ASC
       """.stripMargin.replaceAll("\n", "").trim)
  }

  it should "optimize UNION with WHERE queries " in {
    val sql =
      s"""
         | SELECT City, Country FROM Customers
         | WHERE Country='Germany'
         | UNION
         | SELECT City, Country FROM Suppliers
         | WHERE Country='Germany'
         | ORDER BY City
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               | SELECT _u2.City, _u2.Country FROM
                               | (SELECT DISTINCT _u1.City, _u1.Country FROM
                               | (SELECT City, Country FROM Customers WHERE
                               | Country = 'Germany'
                               | UNION ALL
                               | SELECT City, Country FROM Suppliers WHERE
                               | Country = 'Germany') _u1) _u2
                               | ORDER BY City ASC
       """.stripMargin.replaceAll("\n", "").trim)
  }

  it should "optimize UNION ALL with WHERE queries " in {
    val sql =
      s"""
         | SELECT City, Country FROM Customers
         | WHERE Country='Germany'
         | UNION ALL
         | SELECT City, Country FROM Suppliers
         | WHERE Country='Germany'
         | ORDER BY City
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               | SELECT _u2.City, _u2.Country FROM
                               | (SELECT _u1.City, _u1.Country FROM
                               | (SELECT City, Country FROM Customers WHERE
                               | Country = 'Germany'
                               | UNION ALL
                               | SELECT City, Country FROM Suppliers WHERE
                               | Country = 'Germany') _u1) _u2
                               | ORDER BY City ASC
       """.stripMargin.replaceAll("\n", "").trim)
  }

  it should "optimize UNION with additional column" in {
    val sql =
      s"""
         | SELECT 'Customer' As Type, ContactName, City, Country
         | FROM Customers
         | UNION
         | SELECT 'Supplier', ContactName, City, Country
         | FROM Suppliers
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               | SELECT DISTINCT _u1.Type, _u1.ContactName, _u1.City, _u1.Country FROM
                               | (SELECT 'Customer' AS Type, ContactName, City, Country FROM Customers
                               | UNION ALL
                               | SELECT 'Supplier', ContactName, City, Country FROM Suppliers)
                               | _u1
       """.stripMargin.replaceAll("\n", "").trim)
  }

  /********************************* #19 GROUP BY TEST CASES **********************************************/
  it should "optimize queries with group by" in {
    val sql =
      s"""
         |SELECT M.member_id, M.last_name, M.age FROM growingtree.member M WHERE test = true
         | AND M.member_id = 'M-000000001' group by m.age
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT M.member_id, M.last_name, M.age FROM growingtree.member M WHERE test = true
                               | AND M.member_id = 'M-000000001' AND M.last_digits = '01' GROUP BY m.age
           """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with group by and Order by" in {
    val sql =
      s"""
         |SELECT M.member_id, M.last_name, M.age FROM growingtree.member M WHERE test = true
         | AND M.member_id = 'M-000000001' group by m.age order by m.age
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT M.member_id, M.last_name, M.age FROM growingtree.member M WHERE test = true
                               | AND M.member_id = 'M-000000001' AND M.last_digits = '01' GROUP BY m.age ORDER BY m.age ASC
           """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with group by and Order by desc" in {
    val sql =
      s"""
         |SELECT M.member_id, M.last_name, M.age FROM growingtree.member M WHERE test = true
         | AND M.member_id = 'M-000000001' group by m.age order by m.age desc
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT M.member_id, M.last_name, M.age FROM growingtree.member M WHERE test = true
                               | AND M.member_id = 'M-000000001' AND M.last_digits = '01' GROUP BY m.age ORDER BY m.age DESC
           """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with group by and Order by desc no alias" in {
    val sql =
      s"""
         |SELECT member_id, last_name, age FROM growingtree.member WHERE test = true
         | AND member_id = 'M-000000001' group by age order by age desc
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT member_id, last_name, age FROM growingtree.member WHERE test = true
                               | AND member_id = 'M-000000001' AND last_digits = '01' GROUP BY age ORDER BY age DESC
           """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with group by and having" in {
    val sql =
      s"""
         |SELECT member_id, last_name, age FROM growingtree.member WHERE test = true
         | AND member_id = 'M-000000001' GROUP BY age HAVING MAX(field1) - MIN(field2) > 10
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT member_id, last_name, age FROM growingtree.member WHERE test = true
                               | AND member_id = 'M-000000001' AND last_digits = '01' GROUP BY age HAVING MAX(field1) - MIN(field2) > 10
           """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize simple queries with GROUP BY" in {
    val sql =
      s"""
         |SELECT COUNT(CustomerID), Country
         | FROM Customers
         | GROUP BY Country
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT COUNT(CustomerID), Country
                               | FROM Customers
                               | GROUP BY Country
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with GROUP BY and SORTED" in {
    val sql =
      s"""
         |SELECT COUNT(CustomerID), Country
         | FROM Customers
         | GROUP BY Country
         | ORDER BY COUNT(CustomerID) DESC
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT COUNT(CustomerID), Country
                               | FROM Customers
                               | GROUP BY Country
                               | ORDER BY COUNT(CustomerID) DESC
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with GROUP BY and with JOIN" in {
    val sql =
      s"""
         |SELECT Shippers.ShipperName, COUNT(Orders.OrderID) AS NumberOfOrders FROM Orders
         | LEFT JOIN Shippers ON Orders.ShipperID = Shippers.ShipperID
         | GROUP BY ShipperName
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT Shippers.ShipperName, COUNT(Orders.OrderID) AS NumberOfOrders FROM Orders
                               | LEFT JOIN Shippers ON Orders.ShipperID = Shippers.ShipperID
                               | GROUP BY ShipperName
       """.stripMargin.replaceAll("\n", "").trim)

  }

  /*********************************** #20 HAVING TEST CASES ******************************************************/

  it should "optimize simple queries with HAVING" in {
    val sql =
      s"""
         |SELECT COUNT(CustomerID), Country
         | FROM Customers
         | GROUP BY Country
         | HAVING COUNT(CustomerID) > 5
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT COUNT(CustomerID), Country
                               | FROM Customers
                               | GROUP BY Country
                               | HAVING COUNT(CustomerID) > 5
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with HAVING and ORDER BY" in {
    val sql =
      s"""
         |SELECT COUNT(CustomerID), Country
         | FROM Customers
         | GROUP BY Country
         | HAVING COUNT(CustomerID) > 5
         | ORDER BY COUNT(CustomerID) DESC
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT COUNT(CustomerID), Country
                               | FROM Customers
                               | GROUP BY Country
                               | HAVING COUNT(CustomerID) > 5
                               | ORDER BY COUNT(CustomerID) DESC
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with HAVING COUNT" in {
    val sql =
      s"""
         |SELECT Employees.LastName, COUNT(Orders.OrderID) AS NumberOfOrders
         | FROM Orders
         | INNER JOIN Employees ON Orders.EmployeeID = Employees.EmployeeID
         | GROUP BY LastName
         | HAVING COUNT(Orders.OrderID) > 10
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT Employees.LastName, COUNT(Orders.OrderID) AS NumberOfOrders
                               | FROM Orders
                               | INNER JOIN Employees ON Orders.EmployeeID = Employees.EmployeeID
                               | GROUP BY LastName
                               | HAVING COUNT(Orders.OrderID) > 10
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with HAVING COUNT and WHERE condition" in {
    val sql =
      s"""
         |SELECT Employees.LastName, COUNT(Orders.OrderID) AS NumberOfOrders
         | FROM Orders
         | INNER JOIN Employees ON Orders.EmployeeID = Employees.EmployeeID
         | WHERE LastName = 'Davolio' OR LastName = 'Fuller'
         | GROUP BY LastName
         | HAVING COUNT(Orders.OrderID) > 25
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT Employees.LastName, COUNT(Orders.OrderID) AS NumberOfOrders
                               | FROM Orders
                               | INNER JOIN Employees ON Orders.EmployeeID = Employees.EmployeeID
                               | WHERE LastName = 'Davolio' OR LastName = 'Fuller'
                               | GROUP BY LastName
                               | HAVING COUNT(Orders.OrderID) > 25
       """.stripMargin.replaceAll("\n", "").trim)

  }

  /*********************************** #21 SQL EXISTS TEST CASES ******************************************************/

  it should "optimize queries with EXISTS in subquery" in {
    val sql =
      s"""
         |SELECT SupplierName
         | FROM Suppliers
         | WHERE EXISTS (SELECT ProductName FROM Products WHERE SupplierId = Suppliers.supplierId AND Price < 20)
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT SupplierName
                               | FROM Suppliers
                               | WHERE EXISTS (SELECT ProductName FROM Products WHERE SupplierId = Suppliers.supplierId
                               | AND Price < 20)
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with EXISTS in subquery with conditions" in {
    val sql =
      s"""
         |SELECT SupplierName
         | FROM Suppliers
         | WHERE EXISTS (SELECT ProductName FROM Products WHERE SupplierId = Suppliers.supplierId AND Price = 22)
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT SupplierName
                               | FROM Suppliers
                               | WHERE EXISTS (SELECT ProductName FROM Products WHERE SupplierId = Suppliers.supplierId
                               | AND Price = 22)
       """.stripMargin.replaceAll("\n", "").trim)

  }

  /*********************************** #22 SELECT CASE TEST CASES ******************************************************/

  it should "optimize SELECT CASE queries" in {
    val sql =
      s"""
         |SELECT OrderID, Quantity,
         | CASE
         | WHEN Quantity > 30 THEN "The quantity is greater than 30"
         | WHEN Quantity = 30 THEN "The quantity is 30"
         | ELSE "The quantity is under 30"
         | END AS QuantityText
         | FROM OrderDetails
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT OrderID, Quantity,
                               | CASE
                               | WHEN Quantity > 30 THEN "The quantity is greater than 30"
                               | WHEN Quantity = 30 THEN "The quantity is 30"
                               | ELSE "The quantity is under 30"
                               | END AS QuantityText
                               | FROM OrderDetails
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize SELECT with CASE in ORDER BY queries" in {
    val sql =
      s"""
         |SELECT CustomerName, City, Country
         | FROM Customers
         | ORDER BY
         | CASE
         | WHEN City IS NULL THEN Country
         | ELSE City
         | END
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT CustomerName, City, Country
                               | FROM Customers
                               | ORDER BY
                               | CASE
                               | WHEN City IS NULL THEN Country
                               | ELSE City
                               | END ASC
       """.stripMargin.replaceAll("\n", "").trim)

  }

  /************* #23 SQL IFNULL, ISNULL, COALESCE, and NVL Functions TEST CASES ******************************/

  it should "optimize queries with IFNULL" in {
    val sql =
      s"""
         |SELECT ProductName, UnitPrice * (UnitsInStock + IFNULL(UnitsOnOrder, 0))
         | FROM Products
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT ProductName, UnitPrice * (UnitsInStock + IFNULL(UnitsOnOrder, 0))
                               | FROM Products
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with COALESCE" in {
    val sql =
      s"""
         |SELECT ProductName, UnitPrice * (UnitsInStock + COALESCE(UnitsOnOrder, 0))
         | FROM Products
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT ProductName, UnitPrice * (UnitsInStock + COALESCE(UnitsOnOrder, 0))
                               | FROM Products
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with ISNULL" in {
    val sql =
      s"""
         |SELECT ProductName, UnitPrice * (UnitsInStock + IIF(IsNull(UnitsOnOrder), 0, UnitsOnOrder))
         | FROM Products
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT ProductName, UnitPrice * (UnitsInStock + IIF(IsNull(UnitsOnOrder), 0, UnitsOnOrder))
                               | FROM Products
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize queries with NVL" in {
    val sql =
      s"""
         |SELECT ProductName, UnitPrice * (UnitsInStock + NVL(UnitsOnOrder, 0))
         | FROM Products
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT ProductName, UnitPrice * (UnitsInStock + NVL(UnitsOnOrder, 0))
                               | FROM Products
       """.stripMargin.replaceAll("\n", "").trim)

  }

  /*********************************** #24 SUBQUERIES TEST CASES ******************************************************/

  it should "optimize simple subqueries" in {
    val sql =
      s"""
         |SELECT * FROM member WHERE member_id in(select member_id from activity WHERE test = true)
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT * FROM member WHERE member_id IN (SELECT member_id FROM activity WHERE test = true)
           """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize complex queries with subqueries" in {
    val sql =
      s"""
         |select distinct m.first_name, m.last_name from member m, activity a
         |  where
         |       m.member_id = a.member_id and
         |       (array_contains(m.area_tags, 'San Jose, CA') or
         |       array_contains(m.area_tags, 'San Francisco--Oakland, CA')) and
         |       a.activity_date>cast('2018-02-18' as date) and
         |       m.member_id not in
         |           (select member_id from event where event_type='click' and
         |            from_utc_timestamp(event_utc_time, local_tz)>
         |               cast('2019-01-01' as timestamp))
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT DISTINCT m.first_name, m.last_name FROM member m INNER JOIN activity a
                               | WHERE
                               | ((m.member_id = a.member_id AND
                               | (array_contains(m.area_tags, 'San Jose, CA') OR
                               | array_contains(m.area_tags, 'San Francisco--Oakland, CA'))) AND
                               | a.activity_date > CAST('2018-02-18' AS DATE)) AND
                               | NOT m.member_id IN
                               | (SELECT member_id FROM event WHERE event_type = 'click' AND
                               | from_utc_timestamp(event_utc_time, local_tz) >
                               | CAST('2019-01-01' AS TIMESTAMP))
       """.stripMargin.replaceAll("\n", "").trim)

  }

  it should "optimize complex queries with subqueries #2 " in {
    val sql =
      s"""
         |select distinct m.first_name, m.last_name from member m, activity a
         |  where
         |       m.member_id = a.member_id and
         |       a.activity_date > cast('2018-02-18' as date) and
         |       m.member_id not in
         |           (select member_id from event where event_type='click' and
         |            from_utc_timestamp(event_utc_time, local_tz)>
         |               cast('2019-01-01' as timestamp))
       """.stripMargin

    val ret = HiveQueryOptimizer.optimize(sql)

    ret.optimized should equal(true)
    ret.result should equal(s"""
                               |SELECT DISTINCT m.first_name, m.last_name FROM member m INNER JOIN activity a
                               | WHERE
                               | (m.member_id = a.member_id AND
                               | a.activity_date > CAST('2018-02-18' AS DATE)) AND
                               | NOT m.member_id IN
                               | (SELECT member_id FROM event WHERE event_type = 'click' AND
                               | from_utc_timestamp(event_utc_time, local_tz) >
                               | CAST('2019-01-01' AS TIMESTAMP))
       """.stripMargin.replaceAll("\n", "").trim)

  }

}
