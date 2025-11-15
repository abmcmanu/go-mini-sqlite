### Mini Data Engine

This mini relational database engine is written in **Go**, inspired by **SQLite**.
It allows you to **create databases and tables**, **insert**, **select**, **update**, and **delete data**, all **in-memory** with **disk persistence**.

This is an **educational project** built to understand the core concepts behind **database management systems (DBMS)** such as indexing, table structures, schema design, SQL parsing, and persistence.

---

### 🧩 Features

- Support for **multiple databases**  
- **Tables with customizable schemas** (`INT`, `STRING`, with `PRIMARY KEY`, `UNIQUE`, `NOT NULL` constraints)  
- **Logical indexing** through a simplified **B+Tree**  
- Supported SQL commands:
  - `CREATE DATABASE`
  - `DROP DATABASE`
  - `SHOW DATABASES`
  - `USE <database>`
  - `CREATE TABLE`
  - `DROP TABLE`
  - `SHOW TABLES`
  - `DESCRIBE / DESC <table>`
  - `INSERT INTO`
  - `SELECT ... WHERE ...`
  - `UPDATE ... SET ... WHERE ...`
  - `DELETE FROM ... WHERE ...`
- **Data persistence** on disk via `.gob` files  
- **Minimal interactive shell (REPL)**

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
- No support yet for `JOIN` or `ORDER BY`
- The B+Tree is **logical and simplified**, not optimized for large databases
- The shell only supports **single-line commands ending with `;`**
- `UPDATE` and `DELETE` require a `WHERE` clause

---

> 🧪 *A hands-on project to explore how relational databases work from the inside out — from parsing to persistence.*
