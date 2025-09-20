const express = require('express');
const multer = require('multer');
const XLSX = require('xlsx');
const { Pool } = require('pg');
const path = require('path');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

// Database connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

// Middleware
app.use(express.static('public'));
app.use(express.json());

// Configure multer for file uploads
const storage = multer.memoryStorage();
const upload = multer({ 
  storage: storage,
  fileFilter: (req, file, cb) => {
    const allowedTypes = [
      'application/vnd.ms-excel',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    ];
    if (allowedTypes.includes(file.mimetype) || file.originalname.match(/\.(xlsx|xls)$/)) {
      cb(null, true);
    } else {
      cb(new Error('Only Excel files are allowed'), false);
    }
  }
});

// Initialize database table
async function initDatabase() {
  try {
    // Drop the old table and create the new one with correct structure
    await pool.query('DROP TABLE IF EXISTS records');
    await pool.query('DROP TABLE IF EXISTS upload_log');
    
    await pool.query(`
      CREATE TABLE IF NOT EXISTS records (
        id SERIAL PRIMARY KEY,
        order_date DATE,
        cus_no VARCHAR(50),
        customer_name VARCHAR(255),
        title VARCHAR(500),
        book_ean VARCHAR(20),
        quantity INTEGER,
        total DECIMAL(10,2),
        upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    await pool.query(`
      CREATE TABLE IF NOT EXISTS upload_log (
        id SERIAL PRIMARY KEY,
        filename VARCHAR(255),
        records_count INTEGER,
        upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    console.log('Database initialized successfully');
  } catch (error) {
    console.error('Database initialization error:', error);
  }
}

// Routes

// Serve the main page
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Upload and process Excel file
app.post('/upload', upload.single('excelFile'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    // Read Excel file
    const workbook = XLSX.read(req.file.buffer, { type: 'buffer' });
    const sheetName = workbook.SheetNames[0]; // Use first sheet
    const worksheet = workbook.Sheets[sheetName];
    
    // Convert to JSON, starting from row 2 (skipping header row)
    const data = XLSX.utils.sheet_to_json(worksheet, { range: 1 });
    
    console.log('Extracted data sample:', data.slice(0, 3)); // Log first 3 rows for debugging
    console.log('Column headers:', Object.keys(data[0] || {})); // Log column names
    
    // Process and insert data
    const insertedRecords = [];
    
    for (const row of data) {
      // Skip empty rows
      if (!row || Object.keys(row).length === 0) continue;
      
      // Get all column keys to understand the structure
      const keys = Object.keys(row);
      console.log('Processing row with keys:', keys);
      console.log('Row data:', row);
      
      // Map your specific Excel columns to database fields
      // Based on your original Excel structure, adjust these mappings
      const record = {
        order_date: parseDate(row[keys[0]] || new Date()), // First column (Date)
        cus_no: row[keys[2]] || null, // Third column (Cus No)
        customer_name: row[keys[3]] || 'Unknown', // Fourth column (Customer Name)
        title: row[keys[5]] || 'Unknown', // Sixth column (Title)
        book_ean: row[keys[7]] || null, // Eighth column (Book EAN)
        quantity: parseInt(row[keys[8]]) || 0, // Ninth column (Quantity)
        total: parseFloat(row[keys[9]]) || 0 // Tenth column (Total)
      };

      const result = await pool.query(
        'INSERT INTO records (order_date, cus_no, customer_name, title, book_ean, quantity, total) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *',
        [record.order_date, record.cus_no, record.customer_name, record.title, record.book_ean, record.quantity, record.total]
      );
      
      insertedRecords.push(result.rows[0]);
    }

    // Log the file upload
    await pool.query(
      'INSERT INTO upload_log (filename, records_count) VALUES ($1, $2)',
      [req.file.originalname, insertedRecords.length]
    );

    res.json({ 
      message: `Successfully processed ${insertedRecords.length} records`, 
      records: insertedRecords 
    });

  } catch (error) {
    console.error('Upload error:', error);
    res.status(500).json({ error: 'Failed to process file: ' + error.message });
  }
});

// Get all records
app.get('/records', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM records ORDER BY upload_date DESC');
    res.json(result.rows);
  } catch (error) {
    console.error('Fetch records error:', error);
    res.status(500).json({ error: 'Failed to fetch records' });
  }
});

// Get upload log
app.get('/upload-log', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM upload_log ORDER BY upload_date DESC');
    res.json(result.rows);
  } catch (error) {
    console.error('Fetch upload log error:', error);
    res.status(500).json({ error: 'Failed to fetch upload log' });
  }
});

// Delete a record (optional - for testing)
app.delete('/records/:id', async (req, res) => {
  try {
    await pool.query('DELETE FROM records WHERE id = $1', [req.params.id]);
    res.json({ message: 'Record deleted successfully' });
  } catch (error) {
    console.error('Delete error:', error);
    res.status(500).json({ error: 'Failed to delete record' });
  }
});

// Utility function to parse dates
function parseDate(dateValue) {
  if (!dateValue) return new Date();
  if (dateValue instanceof Date) return dateValue;
  
  // Try to parse various date formats
  const parsed = new Date(dateValue);
  return isNaN(parsed.getTime()) ? new Date() : parsed;
}

// Start server
initDatabase().then(() => {
  app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
  });
});
