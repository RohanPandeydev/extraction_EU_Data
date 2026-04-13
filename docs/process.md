# Process Documentation

## 1. Data Extraction

- Extracted data from the European Union Medical Devices (EUDAMED) API using Node.js and the `fetch` API.
- Implemented pagination and rate limiting to avoid API throttling.

## 2. Database Table Creation

- Designed the `devices_specific` table in PostgreSQL to store device data.
- Used appropriate data types: `VARCHAR` for strings, `JSONB` for objects/arrays, and avoided using data types in `INSERT INTO` statements.

## 3. Data Insertion and Upsert Logic

- Inserted device data into the database.
- Implemented logic to check if a device already exists (by UUID). If present, updated the record; if not, inserted a new record. This prevents duplicate key errors.

## 4. Error Handling and Testing

- Handled errors related to data types and duplicate keys.
- Verified the process by running the script and checking the database for correct data insertion and updates.

## Summary

The process involved extracting, transforming, and loading (ETL) EUDAMED device data into a PostgreSQL database, with careful handling of data types and upsert logic to ensure data integrity.
