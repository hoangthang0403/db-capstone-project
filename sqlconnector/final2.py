import mysql.connector as connector
from mysql.connector.pooling import MySQLConnectionPool

# ============= TASK 1 =============
print("============= TASK 1 ============= ")
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


# ============= TASK 2 ============= 
print("============= TASK 2 ============= ")
connections = []
try:
    # FIRST GUEST
    first_guest = pool.get_connection()
    cursor1 = first_guest.cursor(buffered = True)
    # cursor1.execute("USE little_lemon_db;")
    # cursor1.execute("""INSERT INTO bookings (TableNo,GuestFirstName,GuestLastName,BookingSlot,EmployeeID) VALUES (8,'Anees','Java','18:00:00',6)""")
    # first_guest.commit()
    print("First guest made reservation successfully")
    connections.append(first_guest)

    # SECOND GUEST
    second_guest = pool.get_connection()
    cursor2 = second_guest.cursor(buffered = True)
    # cursor2.execute("USE little_lemon_db;")
    # cursor2.execute("""INSERT INTO bookings (TableNo,GuestFirstName,GuestLastName,BookingSlot,EmployeeID) VALUES (5,'Bald','Vin','19:00:00',6)""")
    # second_guest.commit()
    print("Second made reservation successfully")
    connections.append(second_guest)

    # ADDING NEW CONNECTION
    connection = connector.connect(
            user = "hoangthang0403",
            password = "04032003"   
        )
    pool.add_connection(cnx = connection)

    # THIRD GUEST
    third_guest = pool.get_connection()
    cursor3 = third_guest.cursor(buffered = True)
    # cursor3.execute("USE little_lemon_db;")
    # cursor3.execute("""INSERT INTO bookings (TableNo,GuestFirstName,GuestLastName,BookingSlot,EmployeeID) VALUES (12,'Jay','Kon','19:30:00',6)""")
    # third_guest.commit()
    print("Third guest made reservation successfully")
    connections.append(third_guest)

    # RETURNING ALL CONNECTIONS TO THE POOL
    for connection in connections:
        connection.close()

except connector.Error as er:
    print("Error code:",er.errno)
    print("Error msg:",er.msg)
print("-------------------")

# ============= TASK 3 ============= 
print("============= TASK 3 ============= ")
connection = pool.get_connection()
cursor = connection.cursor(buffered = True)

get_manager = """SELECT EmployeeID, Name FROM employees WHERE Role = 'Manager';"""
get_highest_salary_employee = """SELECT Name,Role FROM employees ORDER BY Annual_Salary DESC LIMIT 1;"""
get_number_of_guests_between_1820 = """SELECT COUNT(BookingID) AS NumberOfGuests, HOUR(BookingSlot) as Hour FROM bookings WHERE BookingSlot BETWEEN '18:00:00' AND '20:00:00' GROUP BY Hour ORDER BY Hour;"""
get_guests_booking_detail = """SELECT BookingID, CONCAT(GuestFirstName, ' ', GuestLastName) as FullName, HOUR(BookingSlot) Hour FROM bookings b JOIN employees e ON b.EmployeeID = e.EmployeeID WHERE Role ='Receptionist' ORDER BY Hour"""

cursor.execute(get_manager)
results = cursor.fetchall()
print("Manager Information")
print(cursor.column_names)
for row in results:
    print(row)
results.clear()
print("-------------------")

cursor.execute(get_highest_salary_employee)
results = cursor.fetchall()
print("Employee with highest salary")
print(cursor.column_names)
for row in results:
    print(row)
results.clear()
print("-------------------")

cursor.execute(get_number_of_guests_between_1820)
results = cursor.fetchall()
print("Number of guests between 18:00 and 20:00")
print(cursor.column_names)
for row in results:
    print(row)
results.clear()
print("-------------------")

cursor.execute(get_guests_booking_detail)
results = cursor.fetchall()
print("Guest booking details")
print(cursor.column_names)
for row in results:
    print(row)
results.clear()
print("-------------------")

# ============= TASK 4 ============= 
print("============= TASK 4 ============= ")
BasicSalesReport = """
CREATE PROCEDURE BasicSalesReport()
BEGIN
SELECT SUM(BillAmount) AS TotalSales, AVG(BillAmount) AS AverageSale, MIN(BillAmount) AS MininumBill, MAX(BillAmount) AS MaximumBill
FROM orders;
END
"""
# cursor.execute(BasicSalesReport)
print("Calling procedure")
cursor.callproc("BasicSalesReport")
result = next(cursor.stored_results())
dataset = result.fetchall()
print("Basic Sales Report")
print(result.column_names)
for row in dataset:
    print(row)
cursor.fetchall()
print("-------------------")

# ============= TASK 5 =============
print("============= TASK 5 ============= ")
chef_connection = pool.get_connection()
cursor4 = chef_connection.cursor(buffered = True)
cursor4.execute("""SELECT BookingSlot, CONCAT(GuestFirstName,' ', GuestLastName) as Guest_name, CONCAT(Name, ' [', Role,']') as 'Assigned to: Employee Name [Employee Role]'
                FROM bookings b JOIN employees e ON b.EmployeeID = e.EmployeeID ORDER BY BookingSlot LIMIT 3; """)
results = cursor4.fetchall()
for row in results:
    print("BookingSlot ",row[0],"\n\tGuest_name: ",row[1],"\n\tAssigned to: ",row[2])
results.clear()
print("-------------------")

cursor.close()
connection.close()
cursor4.close()
chef_connection.close()