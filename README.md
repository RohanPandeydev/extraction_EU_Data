# eudamed-data-extraction

This project extracts and loads data from the European Union Medical Devices (EUDAMED) API into a PostgreSQL database.

## Features

- Fetches device data from the EUDAMED API with pagination and rate limiting
- Stores device data in a PostgreSQL database with proper data type handling
- Upserts (inserts or updates) device records to avoid duplicate key errors
- Handles complex fields using JSONB columns

## Setup

1. **Clone the repository**
2. **Install dependencies**
   ```bash
   npm install
   ```
3. **Configure the database**
   - Set your PostgreSQL credentials in a `.env` file:
     ```
     DB_USER=youruser
     DB_HOST=localhost
     DB_DATABASE=yourdb
     DB_PASSWORD=yourpassword
     DB_PORT=5432
     ```
4. **Run the script**
   ```bash
   node script.js
   ```

## Troubleshooting

- If you encounter errors related to data types, ensure your table schema uses `VARCHAR` for strings and `JSONB` for objects/arrays.
- For duplicate key errors, the script will update existing records instead of inserting duplicates.

## Documentation

- See `docs/process.md` for the workflow and process details.
- See `docs/problems.md` for problems faced and solutions.

## License

MIT
