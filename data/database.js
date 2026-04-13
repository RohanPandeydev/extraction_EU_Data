require("dotenv").config();
const pg = require("pg");
const { Pool } = pg;

const pool = new Pool({
  user: process.env.DB_USERNAME || "shubham",
  host: process.env.DB_HOST || "localhost",
  database: process.env.DB_DATABASE || "gs_eudamed",
  password: process.env.DB_PASSWORD || "shubham",
  port: process.env.DB_PORT ? parseInt(process.env.DB_PORT) : 5432,
});

module.exports = {
  query: (text, params) => pool.query(text, params),
};
