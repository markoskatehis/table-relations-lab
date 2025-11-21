import sqlite3
import pandas as pd

conn = sqlite3.connect('data.sqlite')

pd.read_sql("""SELECT * FROM sqlite_master""", conn)

sql_boston = """
SELECT e.firstName, e.lastName
FROM employees e
JOIN offices o ON e.officeCode = o.officeCode
WHERE o.city = 'Boston'
ORDER BY e.firstName, e.lastName;
"""
df_boston = pd.read_sql(sql_boston, conn)
    
sql_zero_emp = """
SELECT o.officeCode, o.city
FROM offices o
LEFT JOIN employees e ON o.officeCode = e.officeCode
GROUP BY o.officeCode, o.city
HAVING COUNT(e.employeeNumber) = 0;
"""
df_zero_emp = pd.read_sql(sql_zero_emp, conn)

sql_employee = """
SELECT e.firstName, e.lastName, o.city, o.state
FROM employees e
LEFT JOIN offices o ON e.officeCode = o.officeCode
ORDER BY e.firstName ASC, e.lastName ASC;
"""
df_employee = pd.read_sql(sql_employee, conn)

sql_contacts = """
SELECT c.contactFirstName, 
       c.contactLastName, 
       c.phone, 
       c.salesRepEmployeeNumber
FROM customers c
LEFT JOIN orders o ON c.customerNumber = o.customerNumber
WHERE o.orderNumber IS NULL
ORDER BY c.contactLastName ASC;
"""
df_contacts = pd.read_sql(sql_contacts, conn)

sql_payment = """
SELECT c.contactFirstName,
       c.contactLastName,
       p.paymentDate,
       p.amount
FROM payments p
JOIN customers c ON p.customerNumber = c.customerNumber
ORDER BY CAST(p.amount AS REAL) DESC;
"""
df_payment = pd.read_sql(sql_payment, conn)

sql_credit = """
SELECT e.employeeNumber,
       e.firstName,
       e.lastName,
       COUNT(c.customerNumber) AS num_customers
FROM employees e
JOIN customers c ON c.salesRepEmployeeNumber = e.employeeNumber
GROUP BY e.employeeNumber, e.firstName, e.lastName
HAVING AVG(CAST(c.creditLimit AS REAL)) > 90000
ORDER BY num_customers DESC
LIMIT 4;
"""
df_credit = pd.read_sql(sql_credit, conn)

sql_product_sold = """
SELECT p.productName,
       COUNT(od.orderNumber) AS numorders,
       SUM(od.quantityOrdered) AS totalunits
FROM products p
JOIN orderdetails od ON p.productCode = od.productCode
GROUP BY p.productCode, p.productName
ORDER BY totalunits DESC;
"""
df_product_sold = pd.read_sql(sql_product_sold, conn)

sql_total_customers = """
SELECT p.productName,
       p.productCode,
       COUNT(DISTINCT o.customerNumber) AS numpurchasers
FROM products p
JOIN orderdetails od ON p.productCode = od.productCode
JOIN orders o ON od.orderNumber = o.orderNumber
GROUP BY p.productCode, p.productName
ORDER BY numpurchasers DESC;
"""
df_total_customers = pd.read_sql(sql_total_customers, conn)

sql_customers_per_office = """
SELECT o.officeCode,
       o.city,
       COUNT(DISTINCT c.customerNumber) AS n_customers
FROM offices o
LEFT JOIN employees e ON o.officeCode = e.officeCode
LEFT JOIN customers c ON c.salesRepEmployeeNumber = e.employeeNumber
GROUP BY o.officeCode, o.city
ORDER BY o.officeCode ASC;
"""
df_customers = pd.read_sql(sql_customers_per_office, conn)

sql_under_20 = """
WITH product_customers AS (
    SELECT od.productCode,
           COUNT(DISTINCT o.customerNumber) AS num_customers
    FROM orderdetails od
    JOIN orders o ON od.orderNumber = o.orderNumber
    GROUP BY od.productCode
    HAVING COUNT(DISTINCT o.customerNumber) <= 19
)
SELECT DISTINCT e.employeeNumber,
       e.firstName,
       e.lastName,
       off.city AS office_city,
       e.officeCode
FROM product_customers pc
JOIN orderdetails od ON od.productCode = pc.productCode
JOIN orders o ON od.orderNumber = o.orderNumber
JOIN customers c ON o.customerNumber = c.customerNumber
JOIN employees e ON c.salesRepEmployeeNumber = e.employeeNumber
LEFT JOIN offices off ON e.officeCode = off.officeCode
ORDER BY e.lastName ASC, e.firstName ASC;
"""
df_under_20 = pd.read_sql(sql_under_20, conn)

conn.close()