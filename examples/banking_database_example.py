"""
Test with Banking/Financial database - demonstrates financial transaction validation.
This shows real-world banking scenario with accounts, transactions, and fraud detection.
"""

import sqlite3
import os
from datetime import datetime, timedelta
import random
from db_expectations import DatabaseValidator
from db_expectations.suites import ExpectationSuites

DB_PATH = "banking.db"

print("="*70)
print("BANKING & FINANCIAL TRANSACTIONS VALIDATION TEST")
print("="*70)

# Create banking database with realistic data
print("\nCreating banking database with sample data...")

if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Create schema
cursor.execute("""
    CREATE TABLE customers (
        customer_id INTEGER PRIMARY KEY,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        email TEXT NOT NULL UNIQUE,
        phone TEXT,
        date_of_birth TEXT NOT NULL,
        country TEXT NOT NULL,
        registration_date TEXT NOT NULL,
        kyc_status TEXT NOT NULL CHECK (kyc_status IN ('pending', 'verified', 'rejected'))
    )
""")

cursor.execute("""
    CREATE TABLE accounts (
        account_id INTEGER PRIMARY KEY,
        customer_id INTEGER NOT NULL,
        account_type TEXT NOT NULL CHECK (account_type IN ('checking', 'savings', 'investment')),
        balance REAL NOT NULL CHECK (balance >= 0),
        currency TEXT NOT NULL DEFAULT 'USD',
        status TEXT NOT NULL CHECK (status IN ('active', 'inactive', 'frozen')),
        opened_date TEXT NOT NULL,
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    )
""")

cursor.execute("""
    CREATE TABLE transactions (
        transaction_id INTEGER PRIMARY KEY,
        account_id INTEGER NOT NULL,
        transaction_type TEXT NOT NULL CHECK (transaction_type IN ('deposit', 'withdrawal', 'transfer', 'payment')),
        amount REAL NOT NULL CHECK (amount > 0),
        balance_after REAL NOT NULL,
        transaction_date TEXT NOT NULL,
        description TEXT,
        merchant_category TEXT,
        risk_score REAL CHECK (risk_score >= 0 AND risk_score <= 100),
        FOREIGN KEY (account_id) REFERENCES accounts(account_id)
    )
""")

cursor.execute("""
    CREATE TABLE fraud_alerts (
        alert_id INTEGER PRIMARY KEY,
        transaction_id INTEGER NOT NULL,
        alert_type TEXT NOT NULL,
        severity TEXT NOT NULL CHECK (severity IN ('low', 'medium', 'high', 'critical')),
        status TEXT NOT NULL CHECK (status IN ('open', 'investigating', 'resolved', 'false_positive')),
        created_date TEXT NOT NULL,
        resolved_date TEXT,
        FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id)
    )
""")

# Insert sample customers
customers_data = [
    (1, 'John', 'Smith', 'john.smith@email.com', '+1-555-0101', '1985-03-15', 'USA', '2020-01-15', 'verified'),
    (2, 'Emma', 'Johnson', 'emma.j@email.com', '+1-555-0102', '1990-07-22', 'USA', '2020-03-20', 'verified'),
    (3, 'Michael', 'Williams', 'michael.w@email.com', '+1-555-0103', '1982-11-08', 'USA', '2020-05-10', 'verified'),
    (4, 'Sarah', 'Brown', 'sarah.b@email.com', '+1-555-0104', '1995-02-14', 'USA', '2021-01-05', 'verified'),
    (5, 'David', 'Jones', 'david.jones@email.com', '+1-555-0105', '1988-09-30', 'USA', '2021-03-12', 'verified'),
    (6, 'Lisa', 'Garcia', 'lisa.g@email.com', '+1-555-0106', '1992-06-18', 'USA', '2021-06-20', 'pending'),
    (7, 'James', 'Martinez', 'james.m@email.com', '+1-555-0107', '1987-12-05', 'USA', '2022-01-08', 'verified'),
    (8, 'Maria', 'Rodriguez', 'maria.r@email.com', '+1-555-0108', '1993-04-25', 'USA', '2022-04-15', 'verified'),
    (9, 'Robert', 'Lee', 'robert.lee@email.com', '+1-555-0109', '1980-08-12', 'USA', '2022-07-22', 'verified'),
    (10, 'Jennifer', 'Taylor', 'jennifer.t@email.com', '+1-555-0110', '1991-10-03', 'USA', '2023-01-10', 'verified'),
]
cursor.executemany("INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", customers_data)

# Insert accounts
accounts_data = [
    (1, 1, 'checking', 15420.50, 'USD', 'active', '2020-01-15'),
    (2, 1, 'savings', 45000.00, 'USD', 'active', '2020-01-15'),
    (3, 2, 'checking', 8750.25, 'USD', 'active', '2020-03-20'),
    (4, 3, 'checking', 22100.00, 'USD', 'active', '2020-05-10'),
    (5, 3, 'investment', 125000.00, 'USD', 'active', '2020-05-10'),
    (6, 4, 'checking', 3250.75, 'USD', 'active', '2021-01-05'),
    (7, 5, 'checking', 18900.00, 'USD', 'active', '2021-03-12'),
    (8, 5, 'savings', 67000.00, 'USD', 'active', '2021-03-12'),
    (9, 6, 'checking', 1200.50, 'USD', 'inactive', '2021-06-20'),
    (10, 7, 'checking', 9500.00, 'USD', 'active', '2022-01-08'),
    (11, 8, 'checking', 12300.00, 'USD', 'active', '2022-04-15'),
    (12, 9, 'checking', 31000.00, 'USD', 'active', '2022-07-22'),
    (13, 9, 'savings', 98000.00, 'USD', 'active', '2022-07-22'),
    (14, 10, 'checking', 7800.00, 'USD', 'active', '2023-01-10'),
]
cursor.executemany("INSERT INTO accounts VALUES (?, ?, ?, ?, ?, ?, ?)", accounts_data)

# Generate realistic transactions
base_date = datetime(2023, 1, 1)
transaction_id = 1
transactions = []

for account_id in range(1, 15):
    balance = [acc[3] for acc in accounts_data if acc[0] == account_id][0]
    
    # Generate 20-50 transactions per account
    num_transactions = random.randint(20, 50)
    
    for i in range(num_transactions):
        days_offset = random.randint(0, 365)
        trans_date = (base_date + timedelta(days=days_offset)).strftime('%Y-%m-%d %H:%M:%S')
        
        trans_type = random.choice(['deposit', 'withdrawal', 'payment'] + ['transfer'] * 2)
        
        if trans_type == 'deposit':
            amount = round(random.uniform(100, 5000), 2)
            balance += amount
            risk = random.uniform(0, 20)
            merchant = None
        elif trans_type == 'withdrawal':
            amount = round(random.uniform(50, min(balance * 0.3, 2000)), 2)
            balance -= amount
            risk = random.uniform(5, 40)
            merchant = 'ATM'
        elif trans_type == 'payment':
            amount = round(random.uniform(10, min(balance * 0.2, 1000)), 2)
            balance -= amount
            risk = random.uniform(10, 50)
            merchant = random.choice(['groceries', 'restaurant', 'gas_station', 'retail', 'utilities'])
        else:  # transfer
            amount = round(random.uniform(50, min(balance * 0.4, 3000)), 2)
            balance -= amount
            risk = random.uniform(15, 60)
            merchant = 'transfer'
        
        transactions.append((
            transaction_id,
            account_id,
            trans_type,
            amount,
            round(balance, 2),
            trans_date,
            f"{trans_type} transaction",
            merchant,
            round(risk, 2)
        ))
        transaction_id += 1

cursor.executemany("INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", transactions)

# Generate fraud alerts for high-risk transactions
alerts = []
alert_id = 1
for trans in transactions:
    if trans[8] > 70:  # risk_score > 70
        severity = 'critical' if trans[8] > 85 else 'high'
        status = random.choice(['open', 'investigating', 'resolved', 'false_positive'])
        created = trans[5]  # transaction_date
        resolved = (datetime.strptime(created, '%Y-%m-%d %H:%M:%S') + timedelta(days=random.randint(1, 5))).strftime('%Y-%m-%d %H:%M:%S') if status == 'resolved' else None
        
        alerts.append((
            alert_id,
            trans[0],  # transaction_id
            'unusual_amount' if trans[3] > 2000 else 'suspicious_merchant',
            severity,
            status,
            created,
            resolved
        ))
        alert_id += 1

cursor.executemany("INSERT INTO fraud_alerts VALUES (?, ?, ?, ?, ?, ?, ?)", alerts)

conn.commit()
conn.close()

print(f"✓ Created database: {DB_PATH}")
print(f"  - {len(customers_data)} customers")
print(f"  - {len(accounts_data)} accounts")
print(f"  - {len(transactions)} transactions")
print(f"  - {len(alerts)} fraud alerts")

# Connect and validate
connection_string = f"sqlite:///{os.path.abspath(DB_PATH)}"
validator = DatabaseValidator(connection_string)

print("\n" + "="*70)
print("TEST 1: CUSTOMER DATA VALIDATION")
print("="*70)

customer_expectations = ExpectationSuites.combine(
    ExpectationSuites.null_checks(["customer_id", "first_name", "last_name", "email"]),
    ExpectationSuites.unique_checks(["customer_id", "email"]),
    ExpectationSuites.format_checks({"email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"}),
    ExpectationSuites.set_membership_checks({"kyc_status": ["pending", "verified", "rejected"]}),
    ExpectationSuites.row_count_check(min_rows=1)
)

try:
    customer_results = validator.validate_query(
        query="SELECT * FROM customers",
        asset_name="customer_validation",
        suite_name="customer_check",
        expectations=customer_expectations
    )
    
    print(f"Validation: {'✓ PASSED' if customer_results['success'] else '✗ FAILED'}")
    print(f"Success Rate: {customer_results['statistics']['success_percent']:.1f}%")
except Exception as e:
    print(f"✗ Validation failed: {e}")

# Customer statistics
cust_stats = validator.query_to_dataframe("""
    SELECT 
        COUNT(*) as total_customers,
        COUNT(CASE WHEN kyc_status = 'verified' THEN 1 END) as verified_customers,
        COUNT(CASE WHEN kyc_status = 'pending' THEN 1 END) as pending_kyc
    FROM customers
""")
print("\nCustomer Statistics:")
print(cust_stats.to_string(index=False))

print("\n" + "="*70)
print("TEST 2: ACCOUNT VALIDATION")
print("="*70)

account_expectations = ExpectationSuites.combine(
    ExpectationSuites.null_checks(["account_id", "customer_id", "balance"]),
    ExpectationSuites.unique_checks(["account_id"]),
    ExpectationSuites.range_checks({"balance": {"min": 0}}),
    ExpectationSuites.set_membership_checks({
        "account_type": ["checking", "savings", "investment"],
        "status": ["active", "inactive", "frozen"]
    }),
    ExpectationSuites.row_count_check(min_rows=1)
)

try:
    account_results = validator.validate_query(
        query="SELECT * FROM accounts",
        asset_name="account_validation",
        suite_name="account_check",
        expectations=account_expectations
    )
    
    print(f"Validation: {'✓ PASSED' if account_results['success'] else '✗ FAILED'}")
    print(f"Success Rate: {account_results['statistics']['success_percent']:.1f}%")
except Exception as e:
    print(f"✗ Validation failed: {e}")

# Account summary
account_summary = validator.query_to_dataframe("""
    SELECT 
        account_type,
        COUNT(*) as account_count,
        ROUND(SUM(balance), 2) as total_balance,
        ROUND(AVG(balance), 2) as avg_balance,
        COUNT(CASE WHEN status = 'active' THEN 1 END) as active_accounts
    FROM accounts
    GROUP BY account_type
    ORDER BY total_balance DESC
""")
print("\nAccount Summary by Type:")
print(account_summary.to_string(index=False))

print("\n" + "="*70)
print("TEST 3: TRANSACTION VALIDATION")
print("="*70)

transaction_expectations = ExpectationSuites.combine(
    ExpectationSuites.null_checks(["transaction_id", "account_id", "amount", "transaction_date"]),
    ExpectationSuites.unique_checks(["transaction_id"]),
    ExpectationSuites.range_checks({
        "amount": {"min": 0},
        "risk_score": {"min": 0, "max": 100}
    }),
    ExpectationSuites.set_membership_checks({
        "transaction_type": ["deposit", "withdrawal", "transfer", "payment"]
    }),
    ExpectationSuites.row_count_check(min_rows=1)
)

try:
    transaction_results = validator.validate_query(
        query="SELECT * FROM transactions",
        asset_name="transaction_validation",
        suite_name="transaction_check",
        expectations=transaction_expectations
    )
    
    print(f"Validation: {'✓ PASSED' if transaction_results['success'] else '✗ FAILED'}")
    print(f"Success Rate: {transaction_results['statistics']['success_percent']:.1f}%")
except Exception as e:
    print(f"✗ Validation failed: {e}")

# Transaction statistics
trans_stats = validator.query_to_dataframe("""
    SELECT 
        transaction_type,
        COUNT(*) as transaction_count,
        ROUND(SUM(amount), 2) as total_amount,
        ROUND(AVG(amount), 2) as avg_amount,
        ROUND(AVG(risk_score), 2) as avg_risk_score
    FROM transactions
    GROUP BY transaction_type
    ORDER BY total_amount DESC
""")
print("\nTransaction Statistics by Type:")
print(trans_stats.to_string(index=False))

print("\n" + "="*70)
print("TEST 4: HIGH-RISK TRANSACTION DETECTION")
print("="*70)

highrisk_query = """
    SELECT 
        t.transaction_id,
        t.account_id,
        t.transaction_type,
        t.amount,
        t.risk_score,
        t.transaction_date,
        c.first_name || ' ' || c.last_name as customer_name
    FROM transactions t
    JOIN accounts a ON t.account_id = a.account_id
    JOIN customers c ON a.customer_id = c.customer_id
    WHERE t.risk_score > 60
    ORDER BY t.risk_score DESC
    LIMIT 20
"""

highrisk_expectations = ExpectationSuites.combine(
    ExpectationSuites.null_checks(["transaction_id", "risk_score"]),
    ExpectationSuites.range_checks({"risk_score": {"min": 60, "max": 100}})
)

try:
    highrisk_results = validator.validate_query(
        query=highrisk_query,
        asset_name="highrisk_transactions",
        suite_name="highrisk_check",
        expectations=highrisk_expectations
    )
    
    print(f"Validation: {'✓ PASSED' if highrisk_results['success'] else '✗ FAILED'}")
    print(f"Success Rate: {highrisk_results['statistics']['success_percent']:.1f}%")
except Exception as e:
    print(f"✗ Validation failed: {e}")

highrisk_df = validator.query_to_dataframe(highrisk_query)
print(f"\nTop 20 High-Risk Transactions (risk score > 60):")
print(highrisk_df.to_string(index=False))

print("\n" + "="*70)
print("TEST 5: FRAUD ALERT VALIDATION")
print("="*70)

fraud_expectations = ExpectationSuites.combine(
    ExpectationSuites.null_checks(["alert_id", "transaction_id", "severity"]),
    ExpectationSuites.unique_checks(["alert_id"]),
    ExpectationSuites.set_membership_checks({
        "severity": ["low", "medium", "high", "critical"],
        "status": ["open", "investigating", "resolved", "false_positive"]
    }),
    ExpectationSuites.row_count_check(min_rows=0)
)

try:
    fraud_results = validator.validate_query(
        query="SELECT * FROM fraud_alerts",
        asset_name="fraud_validation",
        suite_name="fraud_check",
        expectations=fraud_expectations
    )
    
    print(f"Validation: {'✓ PASSED' if fraud_results['success'] else '✗ FAILED'}")
    print(f"Success Rate: {fraud_results['statistics']['success_percent']:.1f}%")
except Exception as e:
    print(f"✗ Validation failed: {e}")

# Fraud alert summary
fraud_summary = validator.query_to_dataframe("""
    SELECT 
        severity,
        status,
        COUNT(*) as alert_count
    FROM fraud_alerts
    GROUP BY severity, status
    ORDER BY 
        CASE severity 
            WHEN 'critical' THEN 1 
            WHEN 'high' THEN 2 
            WHEN 'medium' THEN 3 
            ELSE 4 
        END,
        status
""")
print("\nFraud Alerts Summary:")
print(fraud_summary.to_string(index=False))

print("\n" + "="*70)
print("TEST 6: CUSTOMER FINANCIAL PROFILE")
print("="*70)

profile_query = """
    SELECT 
        c.customer_id,
        c.first_name || ' ' || c.last_name as customer_name,
        COUNT(DISTINCT a.account_id) as account_count,
        ROUND(SUM(a.balance), 2) as total_balance,
        COUNT(t.transaction_id) as transaction_count,
        ROUND(SUM(CASE WHEN t.transaction_type = 'deposit' THEN t.amount ELSE 0 END), 2) as total_deposits,
        ROUND(SUM(CASE WHEN t.transaction_type IN ('withdrawal', 'payment', 'transfer') THEN t.amount ELSE 0 END), 2) as total_outflows
    FROM customers c
    JOIN accounts a ON c.customer_id = a.customer_id
    LEFT JOIN transactions t ON a.account_id = t.account_id
    WHERE c.kyc_status = 'verified'
    GROUP BY c.customer_id, customer_name
    ORDER BY total_balance DESC
"""

profile_expectations = ExpectationSuites.combine(
    ExpectationSuites.null_checks(["customer_name", "total_balance"]),
    ExpectationSuites.range_checks({
        "account_count": {"min": 1},
        "total_balance": {"min": 0}
    })
)

try:
    profile_results = validator.validate_query(
        query=profile_query,
        asset_name="customer_profile",
        suite_name="profile_check",
        expectations=profile_expectations
    )
    
    print(f"Validation: {'✓ PASSED' if profile_results['success'] else '✗ FAILED'}")
    print(f"Success Rate: {profile_results['statistics']['success_percent']:.1f}%")
except Exception as e:
    print(f"✗ Validation failed: {e}")

profile_df = validator.query_to_dataframe(profile_query)
print("\nCustomer Financial Profiles:")
print(profile_df.to_string(index=False))

print("\n" + "="*70)
print("SUMMARY")
print("="*70)

all_tests = [
    ("Customer Data", customer_results),
    ("Account Data", account_results),
    ("Transactions", transaction_results),
    ("High-Risk Detection", highrisk_results),
    ("Fraud Alerts", fraud_results),
    ("Customer Profiles", profile_results)
]

passed = sum(1 for _, r in all_tests if r["success"])
total = len(all_tests)

print(f"\nTotal Tests: {total}")
print(f"Passed: {passed}")
print(f"Failed: {total - passed}")
print(f"Success Rate: {(passed/total)*100:.1f}%")

print("\nDetailed Results:")
for name, results in all_tests:
    status = "✓ PASSED" if results["success"] else "✗ FAILED"
    print(f"  {name}: {status} ({results['statistics']['success_percent']:.1f}%)")

# Bank-wide insights
print("\n" + "="*70)
print("BANK-WIDE INSIGHTS")
print("="*70)

insights = validator.query_to_dataframe("""
    SELECT 
        (SELECT COUNT(*) FROM customers WHERE kyc_status = 'verified') as verified_customers,
        (SELECT COUNT(*) FROM accounts WHERE status = 'active') as active_accounts,
        (SELECT ROUND(SUM(balance), 2) FROM accounts) as total_deposits,
        (SELECT COUNT(*) FROM transactions) as total_transactions,
        (SELECT ROUND(SUM(amount), 2) FROM transactions) as transaction_volume,
        (SELECT COUNT(*) FROM fraud_alerts WHERE status IN ('open', 'investigating')) as active_fraud_alerts
""")

print(insights.to_string(index=False))

validator.close()
print("\n✓ Validation complete!")
print(f"✓ Database: {DB_PATH}")
