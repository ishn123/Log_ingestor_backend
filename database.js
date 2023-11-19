const mysql = require('mysql2/promise');

class Database {
  constructor(username, password, host, port, databaseName, ...args) {
    this.username = username;
    this.password = password;
    this.host = "localhost";
    this.port = port;
    this.databaseName = databaseName;
    this.connection = null;
    this.connect();
  }

  async connect() {
    try {
      this.connection = await mysql.createConnection({
        host: "localhost",
        database: this.databaseName,
        port: this.port,
        user: this.username,
        password: this.password
      });

      console.log('Database connected successfully');
    } catch (err) {
      console.error('Unable to connect to the database:', err.message);
      process.exit(1);
    }
  }

  async createTables() {
    
    const createQuery = `
          CREATE TABLE IF NOT EXISTS logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            level VARCHAR(50),
            message VARCHAR(255),
            resourceId VARCHAR(50),
            timestamp DATETIME,
            traceId VARCHAR(50),
            spanId VARCHAR(50),
            commit VARCHAR(50),
            parentResourceId VARCHAR(50)
          );
        `;

    try {
      await this.connection.execute(createQuery);
      console.log('Tables created successfully.');
    } catch (e) {
      console.error('Error creating tables:', e.message);
    }
  }


  async insertLog(logData) {
    try {
      const columns = Object.keys(logData).join(', ');
      const valuesTemplate = Object.keys(logData).map(() => '?').join(', ');
      const query = `INSERT INTO logs (${columns}) VALUES (${valuesTemplate})`;

      const values = Object.values(logData);
      
      await this.connection.execute(query, values);
      console.log('Log inserted successfully.');
      return [logData];
    } catch (error) {
      console.error('Error inserting log:', error.message);
      return [{"message":"failed"}];
    }
  }

  async getLogsByFilter(filters) {
    try {
      const conditions = [];
      const values = [];

      for (const [key, value] of Object.entries(filters)) {
        conditions.push(`${key} = ?`);
        values.push(value);
      }

      const query = `SELECT * FROM logs WHERE ${conditions.join(' AND ')}`;

      const [results] = await this.connection.execute(query, values);
      console.log('Logs retrieved successfully:', results);
      return [results];
    } catch (e) {
      console.error('Error retrieving logs:', e.message);
      return []
    }
  }

  async closeConnection() {
    try {
      await this.connection.end();
      console.log('Connection closed successfully.');
    } catch (e) {
      console.error('Error closing connection:', e.message);
    }
  }
}

module.exports = Database;