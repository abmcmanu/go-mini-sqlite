### Mini Data Engine

This mini relational database engine is written in **Go**, inspired by **SQLite**.
It allows you to **create databases and tables**, **insert**, **select**, **update**, and **delete data**, with support for **aggregate functions**, **pattern matching**, and **multiple conditions**, all **in-memory** with **disk persistence**.

This is an **educational project** built to understand the core concepts behind **database management systems (DBMS)** such as indexing, table structures, schema design, SQL parsing, query optimization, and persistence.

---

### 🧩 Features

- Support for **multiple databases**  
- **Tables with customizable schemas** (`INT`, `STRING`, with `PRIMARY KEY`, `UNIQUE`, `NOT NULL` constraints)  
- **Logical indexing** through a simplified **B+Tree**  
- Supported SQL commands:
  - **Database operations:**
    - `CREATE DATABASE <name>`
    - `DROP DATABASE <name>`
    - `SHOW DATABASES`
    - `USE <database>`
  - **Table operations:**
    - `CREATE TABLE <name> (...)`
    - `DROP TABLE <name>`
    - `SHOW TABLES`
    - `DESCRIBE / DESC <table>`
  - **Data manipulation:**
    - `INSERT INTO <table> (...) VALUES (...)`
    - `SELECT * FROM <table> [WHERE ...] [ORDER BY ... [ASC|DESC]] [LIMIT n]`
    - `UPDATE <table> SET ... WHERE ...`
    - `DELETE FROM <table> WHERE ...`
  - **Aggregate functions:**
    - `SELECT COUNT(*) FROM <table> [WHERE ...]`
    - `SELECT SUM(column) FROM <table> [WHERE ...]`
    - `SELECT AVG(column) FROM <table> [WHERE ...]`
  - **WHERE clause operators:**
    - Equality: `column = value`
    - Pattern matching: `column LIKE "pattern"` (supports `%` and `_` wildcards)
    - Multiple conditions: `condition1 AND condition2`, `condition1 OR condition2`
- **Query features:**
  - `ORDER BY` with `ASC`/`DESC` sorting (numeric and alphabetic)
  - `LIMIT` to restrict result count
  - Intelligent type detection for sorting (numeric vs string)
- **Data persistence** on disk via `.gob` files
- **Minimal interactive shell (REPL)**

---

### 📝 Usage Examples

```sql
-- Database setup
CREATE DATABASE myapp;
USE myapp;

-- Create a table
CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    age INT,
    city TEXT
);

-- Insert data
INSERT INTO users (id, name, email, age, city) VALUES ("1", "Alice", "alice@example.com", "30", "Paris");
INSERT INTO users (id, name, email, age, city) VALUES ("2", "Bob", "bob@example.com", "25", "Lyon");
INSERT INTO users (id, name, email, age, city) VALUES ("3", "Charlie", "charlie@gmail.com", "35", "Paris");

-- Basic queries
SELECT * FROM users;
SELECT * FROM users WHERE city="Paris";
SELECT * FROM users WHERE age=30;

-- Pattern matching with LIKE
SELECT * FROM users WHERE email LIKE "%@gmail.com";
SELECT * FROM users WHERE name LIKE "A%";

-- Multiple conditions
SELECT * FROM users WHERE city="Paris" AND age=30;
SELECT * FROM users WHERE city="Paris" OR city="Lyon";
SELECT * FROM users WHERE name LIKE "A%" AND age=30;

-- Sorting and limiting
SELECT * FROM users ORDER BY age ASC;
SELECT * FROM users ORDER BY name DESC LIMIT 2;
SELECT * FROM users WHERE city="Paris" ORDER BY age DESC;

-- Aggregate functions
SELECT COUNT(*) FROM users;
SELECT COUNT(*) FROM users WHERE city="Paris";
SELECT AVG(age) FROM users;
SELECT SUM(age) FROM users WHERE city="Paris";

-- Updates and deletes
UPDATE users SET city="Marseille" WHERE id=2;
UPDATE users SET age="31" WHERE name="Alice" AND city="Paris";
DELETE FROM users WHERE email LIKE "%test%";
DELETE FROM users WHERE age=25 OR age=30;

-- Schema inspection
SHOW TABLES;
DESCRIBE users;
```

---

### 🧠 Concepts & Architecture

#### 1. Data Structures
- **B+Tree** – Logical index for fast row lookup by primary key  
- **Table** – Holds schema, rows, and index  
- **Database** – Manages databases and tables in memory  

#### 2. Schema Constraints
- **PRIMARY KEY** – Unique key per table  
- **NOT NULL** – Mandatory column  
- **UNIQUE** – Ensures unique values in a column  

#### 3. Persistence
- Serialization handled with `encoding/gob`  
- Each table is saved to a `.tbl` file  
- Data is **reloaded into memory** at startup  

#### 4. SQL Parsing
- Uses **regular expressions** to parse SQL commands  
- Builds a **simple AST** (`Statement` interface) with an `Exec` method  

#### 5. Interactive Shell (REPL)
- Reads input line by line  
- Executes SQL commands directly  
- Displays formatted query results  

---

### ⚠️ Current Limitations

- The parser does **not** support multiline statements or comments
- No support yet for `JOIN`
- The B+Tree is **logical and simplified**, not optimized for large databases
- The shell only supports **single-line commands ending with `;`**
- `UPDATE` and `DELETE` require a `WHERE` clause

---

> 🧪 *A hands-on project to explore how relational databases work from the inside out — from parsing to persistence.*
