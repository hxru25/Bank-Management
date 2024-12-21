#dbms
# import os
# import platform
import mysql.connector
import pandas as pd

# MySQL connection setup
mydb = mysql.connector.connect(host="localhost", user="root", password="123456")
mycursor = mydb.cursor(buffered=True)
mycursor.execute("SHOW DATABASES")
result = mycursor.fetchall()  # Fetching results to consume unread data

# Creating necessary tables
mycursor.execute("CREATE DATABASE IF NOT EXISTS bank")
mycursor.execute("USE bank")


mycursor.execute("""CREATE TABLE IF NOT EXISTS account(
                    Accno BIGINT PRIMARY KEY, 
                    Name varchar(255), 
                    Age numeric(3), 
                    Occu varchar(255), 
                    Address varchar(255), 
                    Mob BIGINT, 
                    Aadharno BIGINT, 
                    AccType varchar(255)
                )""")

mycursor.execute("""CREATE TABLE IF NOT EXISTS amt(
                    Accno BIGINT, 
                    Amtdeposite numeric(65),  -- Adjusted precision to 65
                    Month varchar(20), 
                    FOREIGN KEY(Accno) REFERENCES account(Accno)
                )""")

mycursor.execute("""CREATE TABLE IF NOT EXISTS transactions(
                     TransactionID INT AUTO_INCREMENT PRIMARY KEY,
                     Accno BIGINT,
                     TransactionType VARCHAR(50),
                     Amount DECIMAL(65, 2),  -- Adjusted precision to 65
                     TransactionDate DATE,
                     FOREIGN KEY(Accno) REFERENCES account(Accno)
                 )""")

mycursor.execute("""CREATE TABLE IF NOT EXISTS loans(
                     LoanID INT AUTO_INCREMENT PRIMARY KEY,
                     Accno BIGINT,
                     LoanType VARCHAR(50),
                     LoanAmount DECIMAL(65, 2),  -- Adjusted precision to 65
                     FOREIGN KEY(Accno) REFERENCES account(Accno)
                 )""")

mycursor.execute("""CREATE TABLE IF NOT EXISTS beneficiaries(
                     BeneficiaryID INT AUTO_INCREMENT PRIMARY KEY,
                     Accno BIGINT,
                     BeneficiaryName VARCHAR(255),
                     Relationship VARCHAR(50),
                     FOREIGN KEY(Accno) REFERENCES account(Accno)
                 )""")


# Functions for banking operations
def AccInsert():
    Accno = int(input("Enter the Account number : "))
    name = input("Enter the Customer Name: ")
    age = int(input("Enter Age of Customer : "))
    occup = input("Enter the Customer Occupation : ")
    address = input("Enter the Address of the Customer : ")
    mob = int(input("Enter the Mobile number : "))
    aadhar = int(input("Enter the Aadhar number : "))
    acc_type = input("Enter the Account Type (Saving/RD/PPF/Current) : ")

    sql = '''
    INSERT INTO ACCOUNT(Accno, Name, Age, occu, Address, Mob, Aadharno, AccType)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    '''

    values = (Accno, name, age, occup, address, mob, aadhar, acc_type)

    mycursor.execute(sql, values)
    mydb.commit()
    print("Data entered into the database!!")



def AccView():
    print("Select the search criteria:")
    print("1. Acc no")
    print("2. Name")
    print("3. Mobile")
    print("4. Adhar")
    print("5. View All")
    try:
        ch = int(input("Enter the choice: "))
        if ch == 1:
            s = int(input("Enter Account Number: "))
            rl = (s,)
            sql = "SELECT * FROM account WHERE Accno = %s"
            mycursor.execute(sql, rl)
        elif ch == 2:
            s = input("Enter Name: ")
            rl = (s,)
            sql = "SELECT * FROM account WHERE Name = %s"
            mycursor.execute(sql, rl)
        elif ch == 3:
            s = int(input("Enter Mobile No: "))
            rl = (s,)
            sql = "SELECT * FROM account WHERE Mob = %s"
            mycursor.execute(sql, rl)
        elif ch == 4:
            s = input("Enter Adhar: ")
            rl = (s,)
            sql = "SELECT * FROM account WHERE Aadharno = %s"
            mycursor.execute(sql, rl)
        elif ch == 5:
            sql = "SELECT * FROM account"
            mycursor.execute(sql)
        else:
            print("Invalid choice.")
            return
        
        res = mycursor.fetchall()
        if res:
            print("Customer details:")
            for row in res:
                print(row)
        else:
            print("No results found.")
    except ValueError:
        print("Invalid input. Please enter a number.")



def addTransaction():
    Accno = int(input("Enter the Account number : "))
    transaction_type = input("Enter transaction type (Deposit/Withdrawal): ")
    
    if transaction_type.lower() == 'withdrawal':
        # Check if there is enough deposited amount for withdrawal
        amount = float(input("Enter the withdrawal amount: "))
        check_amount_sql = "SELECT SUM(Amtdeposite) FROM amt WHERE Accno = %s"
        mycursor.execute(check_amount_sql, (Accno,))
        available_amount = mycursor.fetchone()[0] or 0

        if available_amount >= amount:
            transaction_date = input("Enter transaction date (YYYY-MM-DD): ")

            # Proceed with withdrawal
            sql = "INSERT INTO transactions(Accno, TransactionType, Amount, TransactionDate) VALUES (%s, %s, %s, %s)"
            values = (Accno, transaction_type, -amount, transaction_date)
            mycursor.execute(sql, values)
            mydb.commit()
            print("Withdrawal successful!")

            # Update withdrawal in 'amt' table
            sql_update = "INSERT INTO amt(Accno, Amtdeposite, Month) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE Amtdeposite = Amtdeposite - %s"
            values_update = (Accno, amount, 'Withdrawal', amount)
            mycursor.execute(sql_update, values_update)
            mydb.commit()
        else:
            print("Insufficient funds for withdrawal.")
    elif transaction_type.lower() == 'deposit':
        amount = float(input("Enter the deposit amount: "))
        transaction_date = input("Enter transaction date (YYYY-MM-DD): ")

        sql = "INSERT INTO transactions(Accno, TransactionType, Amount, TransactionDate) VALUES (%s, %s, %s, %s)"
        values = (Accno, transaction_type, amount, transaction_date)
        mycursor.execute(sql, values)
        mydb.commit()
        print("Deposit recorded successfully!")

        # Update deposit in 'amt' table
        sql_update = "INSERT INTO amt(Accno, Amtdeposite, Month) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE Amtdeposite = Amtdeposite + %s"
        values_update = (Accno, amount, 'Deposit', amount)
        mycursor.execute(sql_update, values_update)
        mydb.commit()
    else:
        print("Invalid transaction type. Enter 'Deposit' or 'Withdrawal'.")



def addLoan():
    Accno = int(input("Enter the Account number : "))
    check_sql = "SELECT COUNT(*) FROM account WHERE Accno = %s"
    mycursor.execute(check_sql, (Accno,))
    result = mycursor.fetchone()
    account_exists = result[0]

    if account_exists:
        # Check if there are any beneficiaries associated with the account
        beneficiary_check_sql = "SELECT COUNT(*) FROM beneficiaries WHERE Accno = %s"
        mycursor.execute(beneficiary_check_sql, (Accno,))
        beneficiary_result = mycursor.fetchone()
        beneficiaries_count = beneficiary_result[0]

        if beneficiaries_count > 0:
            loan_type = input("Enter loan type: ")
            loan_amount = float(input("Enter loan amount: "))

            # Proceed with adding the loan
            sql = "INSERT INTO loans(Accno, LoanType, LoanAmount) VALUES (%s, %s, %s)"
            values = (Accno, loan_type, loan_amount)
            mycursor.execute(sql, values)
            mydb.commit()
            print("Loan added successfully!")
        else:
            print(f"No beneficiaries found for account {Accno}. Cannot proceed with the loan.")
    else:
        print(f"Account number {Accno} does not exist.")


def totalActiveLoans():
    # Calculate total active loans
    total_active_loans_sql = "SELECT SUM(LoanAmount) FROM loans"
    mycursor.execute(total_active_loans_sql)
    total_result = mycursor.fetchone()
    total_active_loan_amount = total_result[0] or 0

    print(f"The total amount of active loans is: {total_active_loan_amount}")



def addBeneficiary():
    Accno = int(input("Enter the Account number : "))
    check_sql = "SELECT COUNT(*) FROM account WHERE Accno = %s"
    mycursor.execute(check_sql, (Accno,))
    result = mycursor.fetchone()
    account_exists = result[0]
    if account_exists:
        beneficiary_name = input("Enter beneficiary name: ")
        relationship = input("Enter relationship with the beneficiary: ")
        
        sql = "INSERT INTO beneficiaries(Accno, BeneficiaryName, Relationship) VALUES (%s, %s, %s)"
        values = (Accno, beneficiary_name, relationship)
        mycursor.execute(sql, values)
        mydb.commit()
        print("Beneficiary added successfully!")
    else:
        print(f"Account number {Accno} does not exist.")



def viewTransactions():
    Accno = int(input("Enter the Account number : "))
    sql = "SELECT * FROM transactions WHERE Accno = %s"
    mycursor.execute(sql, (Accno,))
    result = mycursor.fetchall()
    if result:
        print("Transactions for Account Number", Accno)
        for row in result:
            print(row)
    else:
        print(f"No transactions found for Account number {Accno}.")



def viewLoans():
    Accno = int(input("Enter the Account number : "))
    sql = "SELECT * FROM loans WHERE Accno = %s"
    mycursor.execute(sql, (Accno,))
    result = mycursor.fetchall()
    if result:
        print("Loans for Account Number", Accno)
        for row in result:
            print(row)
    else:
        print(f"No loans found for Account number {Accno}.")



def viewBeneficiaries():
    Accno = int(input("Enter the Account number : "))
    sql = "SELECT * FROM beneficiaries WHERE Accno = %s"
    mycursor.execute(sql, (Accno,))
    result = mycursor.fetchall()
    if result:
        print("Beneficiaries for Account Number", Accno)
        for row in result:
            print(row)
    else:
        print(f"No beneficiaries found for Account number {Accno}.")



def closeAcc():
    Accno = int(input("Enter the Account number of the Customer to be closed : "))
    rl = (Accno,)
    
    # Check if the account number exists in the 'account' table
    check_sql = "SELECT COUNT(*) FROM account WHERE Accno = %s"
    mycursor.execute(check_sql, rl)
    result = mycursor.fetchone()
    account_exists = result[0]
    
    if account_exists:
        # Check if there are any active loans associated with the account
        loan_check_sql = "SELECT COUNT(*) FROM loans WHERE Accno = %s"
        mycursor.execute(loan_check_sql, rl)
        loan_result = mycursor.fetchone()
        active_loans = loan_result[0]

        if active_loans == 0:
            # Delete records from 'transactions', 'beneficiaries', 'account', and 'amt' tables
            delete_transactions_sql = "DELETE FROM transactions WHERE Accno = %s"
            delete_beneficiaries_sql = "DELETE FROM beneficiaries WHERE Accno = %s"
            delete_account_sql = "DELETE FROM account WHERE Accno = %s"
            delete_amt_sql = "DELETE FROM amt WHERE Accno = %s"
            
            mycursor.execute(delete_transactions_sql, rl)
            mycursor.execute(delete_beneficiaries_sql, rl)
            mycursor.execute(delete_account_sql, rl)
            mycursor.execute(delete_amt_sql, rl)

            # Commit the changes
            mydb.commit()
            
            print(f"Account number {Accno} has been closed.")
        else:
            print(f"Cannot close account {Accno}. Active loans exist.")
    else:
        print(f"No records found for Account number {Accno}.")



def MenuSet():
    print("Welcome to Bank Management!!!!")
    print("Select an option:")
    print("1. Add Customer")
    print("2. View Customer Details")
    print("3. Add Loan")
    print("4. Total Active Loans")
    print("5. View Loans")
    print("6. Add Transactions")
    print("7. View Transactions")
    print("8. Add Beneficiary")
    print("9. View Beneficiaries")
    print("10. Close Account")

    try:
        choice = int(input("Enter your choice: "))
        
        if choice == 1:
            AccInsert()
        elif choice == 2:
            AccView()
        elif choice == 3:
            addLoan()
        elif choice == 4:
            totalActiveLoans()
        elif choice == 5:
            viewLoans()
        elif choice == 6:
            addTransaction()
        elif choice == 7:
            viewTransactions()
        elif choice == 8:
            addBeneficiary()
        elif choice == 9:
            viewBeneficiaries()
        elif choice == 10:
            closeAcc()
        else:
            print("Invalid choice. Please enter a valid option.")
    except ValueError:
        print("Please enter a number.")

def runAgain():
    runAgn = input("\nwant To Run Again Y/n: ")
    while(runAgn.lower() == 'y'):
        # if(platform.system() == "Windows"):
        #     print(os.system('cls'))
        # else:
        #     print(os.system('clear'))
        print("\n")
        MenuSet()

# Starting the program
MenuSet()
runAgain()