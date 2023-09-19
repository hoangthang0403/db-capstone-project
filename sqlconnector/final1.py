import mysql.connector as connector
from mysql.connector.pooling import MySQLConnectionPool

dbconfig = {
    "database":"little_lemon_db",
    "user":"hoangthang0403",
    "password":"04032003"
}

try:
    pool = MySQLConnectionPool(pool_name = "pool_b",
                               pool_size=2, #default is 5)
                               **dbconfig)
    print("The connection pool is created with a name:",pool.pool_name)
    print("The pool size is:",pool.pool_size)
except connector.Error as er:
    print("Error code:",er.errno)
    print("Error msg:",er.msg)


connection = pool.get_connection()

cursor = connection.cursor(buffered = True)

# cursor.execute("CREATE DATABASE little_lemon_db;")
cursor.execute("USE little_lemon_db;")

PeakHours = """
CREATE PROCEDURE PeakHours()
BEGIN
    SELECT HOUR(BookingSlot) AS Hour, COUNT(BookingID) AS NumberOfBookings FROM Bookings 
    GROUP BY Hour 
    ORDER BY NumberOfBookings DESC;
END
"""

# cursor.execute(PeakHours)
# cursor.callproc("PeakHours")

GuestStatus = """
CREATE PROCEDURE GuestStatus()
BEGIN
    SELECT CONCAT(GuestFirstName,' ', GuestLastName) as GuestFullName,
    CASE
        WHEN Role IN ('Manager','Assistant Manager') THEN 'Ready to Pay'
        WHEN Role = 'Head Chef' THEN 'Ready to Serve'
        WHEN Role = 'Assistant Chef' THEN 'Preparing Order'
        WHEN Role = 'Head Waiter' THEN 'Order Served'
    END AS Status
    FROM Bookings b LEFT JOIN Employees e ON b.EmployeeID = e.EmployeeID; 
END
"""

# cursor.execute(GuestStatus)
cursor.callproc("GuestStatus")

results = next(cursor.stored_results())

print(results.column_names)
dataset = results.fetchall()

for item in dataset:
    print (item)


if connection.is_connected:
    cursor.close()
    print("The cursor is closed.")
    connection.close()
    print("The connection is closed.")
else:
    print("The connection is already closed")