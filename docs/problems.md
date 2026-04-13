# Problems Faced

## 1. Data Type Issues in PostgreSQL

While inserting data into the `devices_specific` table, I encountered syntax errors related to data types, especially with columns like `version_date` and `discarded_date`. The error was due to mistakenly specifying data types (e.g., `VARCHAR(255)`) in the `INSERT INTO` statement, which is not allowed in SQL. Data types should only be defined in the `CREATE TABLE` statement.

## 2. Array and JSONB Handling

There was confusion between using `JSONB[]` and `JSONB` for columns that store lists or objects. PostgreSQL expects arrays to be handled as `JSONB` for complex data, not as `VARCHAR[]` or `JSONB[]` in this context.

## 3. Duplicate Key Errors

When inserting device data, I faced errors due to duplicate UUIDs violating the unique constraint. This required implementing an upsert (insert or update) logic.

## How I Overcame These Problems

- I corrected the table schema to use `VARCHAR` for string columns and `JSONB` for object/array columns, and ensured no data types were present in the `INSERT INTO` statement.
- I updated the code to check if a record exists before inserting; if it exists, the record is updated instead of inserted, preventing duplicate key errors.
- I tested the changes by running the script and confirming that data was inserted or updated without errors.
