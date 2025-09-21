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
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      'text/csv',
      'application/csv'
    ];
    if (allowedTypes.includes(file.mimetype) || file.originalname.match(/\.(xlsx|xls|csv)$/)) {
      cb(null, true);
    } else {
      cb(new Error('Only Excel (.xlsx, .xls) and CSV files are allowed'), false);
    }
  }
});

// Initialize database
async function initDatabase() {
  try {
    // Create records table with all necessary columns
    await pool.query(`
      CREATE TABLE IF NOT EXISTS records (
        id SERIAL PRIMARY KEY,
        date DATE,
        order_date DATE,
        cust_no VARCHAR(255),
        customer_name VARCHAR(255),
        title VARCHAR(500),
        book_ean VARCHAR(50),
        quantity INTEGER,
        total DECIMAL(10, 2),
        country VARCHAR(100),
        city VARCHAR(100),
        order_reference VARCHAR(255),
        line_identifier VARCHAR(500),
        upload_id INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Create upload_log table
    await pool.query(`
      CREATE TABLE IF NOT EXISTS upload_log (
        id SERIAL PRIMARY KEY,
        filename VARCHAR(255),
        records_count INTEGER,
        upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Create excluded_customers table
    await pool.query(`
      CREATE TABLE IF NOT EXISTS excluded_customers (
        id SERIAL PRIMARY KEY,
        customer_name VARCHAR(255) UNIQUE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Add index for better query performance
    await pool.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_record 
      ON records(order_reference, title, book_ean, quantity, total)
    `);

    console.log('Database initialized successfully');
  } catch (error) {
    console.error('Error initializing database:', error);
  }
}

// Helper function to check if record exists
async function recordExists(orderRef, title, ean, quantity, total) {
  const query = `
    SELECT COUNT(*) as count 
    FROM records 
    WHERE order_reference = $1 
    AND title = $2 
    AND book_ean = $3 
    AND quantity = $4 
    AND total = $5
  `;
  const result = await pool.query(query, [orderRef, title, ean, quantity, total]);
  return result.rows[0].count > 0;
}

// Parse CSV data
function parseCSV(buffer) {
  const text = buffer.toString('utf8');
  const workbook = XLSX.read(text, { type: 'string', raw: true });
  const sheetName = workbook.SheetNames[0];
  const worksheet = workbook.Sheets[sheetName];
  return XLSX.utils.sheet_to_json(worksheet, { header: 1, defval: '' });
}

// Upload endpoint
app.post('/upload', upload.single('excelFile'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    const dataType = req.body.dataType || 'gazelle';
    let data;
    
    // Parse file based on type
    if (req.file.originalname.toLowerCase().endsWith('.csv')) {
      data = parseCSV(req.file.buffer);
    } else {
      const workbook = XLSX.read(req.file.buffer, { type: 'buffer', cellDates: true });
      const sheetName = workbook.SheetNames[0];
      const worksheet = workbook.Sheets[sheetName];
      data = XLSX.utils.sheet_to_json(worksheet, { header: 1, defval: '', dateNF: 'yyyy-mm-dd' });
    }

    if (!data || data.length === 0) {
      return res.status(400).json({ error: 'No data found in file' });
    }

    // Create upload log entry
    const uploadResult = await pool.query(
      'INSERT INTO upload_log (filename, records_count) VALUES ($1, $2) RETURNING id',
      [req.file.originalname, 0]
    );
    const uploadId = uploadResult.rows[0].id;

    let insertedCount = 0;
    let skippedCount = 0;
    const errors = [];

    // Process based on data type
    if (dataType === 'shopify' || req.file.originalname.toLowerCase().endsWith('.csv')) {
      // CSV/Shopify format processing
      const orderGroups = {};
      
      // Group rows by order (Column A - Name)
      for (let i = 1; i < data.length; i++) {
        const row = data[i];
        if (!row || row.length === 0) continue;
        
        const orderName = row[0]; // Column A - Name (Order reference)
        if (!orderName) continue;
        
        if (!orderGroups[orderName]) {
          orderGroups[orderName] = [];
        }
        orderGroups[orderName].push(row);
      }

      // Process each order group
      for (const [orderName, rows] of Object.entries(orderGroups)) {
        let customerName = null;
        
        // Find customer name from first row that has it (Column AM)
        for (const row of rows) {
          if (row[38]) { // Column AM (0-indexed = 38)
            customerName = row[38];
            break;
          }
        }

        // Process each line item in the order
        for (const row of rows) {
          const date = row[15]; // Column P - Date
          const title = row[17]; // Column R - Title
          const quantity = parseInt(row[16]) || 0; // Column Q - Quantity
          const pricePerItem = parseFloat(row[18]) || 0; // Column S - Price per item
          const total = pricePerItem * quantity; // Calculate total
          
          const lineIdentifier = `${orderName}-${title}-${quantity}-${total}`;

          // Check for duplicates
          const exists = await recordExists(orderName, title, '', quantity, total);
          
          if (!exists) {
            try {
              await pool.query(
                `INSERT INTO records (
                  date, order_date, cust_no, customer_name, title, 
                  book_ean, quantity, total, country, city, 
                  order_reference, line_identifier, upload_id
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
                [
                  date, date, '', customerName || 'Unknown', title,
                  '', quantity, total, 'Unknown', 'Unknown',
                  orderName, lineIdentifier, uploadId
                ]
              );
              insertedCount++;
            } catch (err) {
              if (err.code !== '23505') { // Ignore duplicate key errors
                errors.push(`Row error: ${err.message}`);
              } else {
                skippedCount++;
              }
            }
          } else {
            skippedCount++;
          }
        }
      }
    } else {
      // Excel/Gazelle format processing
      for (let i = 1; i < data.length; i++) {
        const row = data[i];
        if (!row || row.length === 0) continue;

        const invoice = row[4]; // Column E - Invoice (Order reference)
        if (!invoice) continue;

        const date = row[0];
        const custNo = row[1] || '';
        const customerName = row[2] || 'Unknown';
        const title = row[5] || '';
        const bookEan = row[6] || '';
        const quantity = parseInt(row[7]) || 0;
        const total = parseFloat(row[8]) || 0;
        
        const lineIdentifier = `${invoice}-${title}-${bookEan}-${quantity}-${total}`;

        // Check for duplicates
        const exists = await recordExists(invoice, title, bookEan, quantity, total);
        
        if (!exists) {
          try {
            await pool.query(
              `INSERT INTO records (
                date, order_date, cust_no, customer_name, title, 
                book_ean, quantity, total, country, city, 
                order_reference, line_identifier, upload_id
              ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
              [
                date, date, custNo, customerName, title,
                bookEan, quantity, total, 'Unknown', 'Unknown',
                invoice, lineIdentifier, uploadId
              ]
            );
            insertedCount++;
          } catch (err) {
            if (err.code !== '23505') { // Ignore duplicate key errors
              errors.push(`Row ${i + 1}: ${err.message}`);
            } else {
              skippedCount++;
            }
          }
        } else {
          skippedCount++;
        }
      }
    }

    // Update upload log with actual count
    await pool.query(
      'UPDATE upload_log SET records_count = $1 WHERE id = $2',
      [insertedCount, uploadId]
    );

    let message = `Successfully processed file. ${insertedCount} new records inserted.`;
    if (skippedCount > 0) {
      message += ` ${skippedCount} duplicate records skipped.`;
    }
    if (errors.length > 0) {
      message += ` ${errors.length} errors occurred.`;
    }

    res.json({ 
      message,
      inserted: insertedCount,
      skipped: skippedCount,
      errors: errors.slice(0, 10) // Return first 10 errors
    });

  } catch (error) {
    console.error('Upload error:', error);
    res.status(500).json({ error: 'Failed to process file: ' + error.message });
  }
});

// Get records with pagination
app.get('/records', async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = 500;
    const offset = (page - 1) * limit;

    // Get total count
    const countResult = await pool.query('SELECT COUNT(*) FROM records');
    const totalRecords = parseInt(countResult.rows[0].count);
    const totalPages = Math.ceil(totalRecords / limit);

    // Get records for current page
    const query = `
      SELECT id, date as order_date, cust_no, customer_name, 
             title, book_ean, quantity, total, country, city 
      FROM records 
      ORDER BY id DESC 
      LIMIT $1 OFFSET $2
    `;
    
    const result = await pool.query(query, [limit, offset]);

    res.json({
      records: result.rows,
      pagination: {
        currentPage: page,
        totalPages: totalPages,
        totalRecords: totalRecords,
        recordsPerPage: limit,
        hasNextPage: page < totalPages,
        hasPreviousPage: page > 1,
        recordsOnThisPage: result.rows.length
      }
    });
  } catch (error) {
    console.error('Error fetching records:', error);
    res.status(500).json({ error: 'Failed to fetch records' });
  }
});

// Get upload log
app.get('/upload-log', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM upload_log ORDER BY upload_date DESC LIMIT 20');
    res.json(result.rows);
  } catch (error) {
    console.error('Error fetching upload log:', error);
    res.status(500).json({ error: 'Failed to fetch upload log' });
  }
});

// Clear all data
app.post('/clear-data', async (req, res) => {
  try {
    await pool.query('DELETE FROM records');
    await pool.query('DELETE FROM upload_log');
    res.json({ message: 'All data cleared successfully' });
  } catch (error) {
    console.error('Error clearing data:', error);
    res.status(500).json({ error: 'Failed to clear data' });
  }
});

// Update record endpoint
app.post('/api/update-record', async (req, res) => {
  try {
    const { id, customer_name, country, city, title } = req.body;
    
    const query = `
      UPDATE records 
      SET customer_name = $1, country = $2, city = $3, title = $4
      WHERE id = $5
      RETURNING *
    `;
    
    const result = await pool.query(query, [customer_name, country, city, title, id]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Record not found' });
    }
    
    res.json({ message: 'Record updated successfully', record: result.rows[0] });
  } catch (error) {
    console.error('Error updating record:', error);
    res.status(500).json({ error: 'Failed to update record' });
  }
});

// Get all customers with aggregated data
app.get('/api/customers', async (req, res) => {
  try {
    const query = `
      SELECT 
        customer_name,
        country,
        city,
        COUNT(DISTINCT order_reference) as order_count,
        SUM(quantity) as total_quantity,
        SUM(total) as total_revenue,
        MAX(date) as last_order_date
      FROM records
      WHERE customer_name IS NOT NULL
      GROUP BY customer_name, country, city
      ORDER BY customer_name
    `;
    
    const result = await pool.query(query);
    res.json(result.rows);
  } catch (error) {
    console.error('Error fetching customers:', error);
    res.status(500).json({ error: 'Failed to fetch customers' });
  }
});

// Get available titles
app.get('/api/titles', async (req, res) => {
  try {
    const query = `
      SELECT DISTINCT title 
      FROM records 
      WHERE title IS NOT NULL AND title != ''
      ORDER BY title
    `;
    
    const result = await pool.query(query);
    res.json(result.rows);
  } catch (error) {
    console.error('Error fetching titles:', error);
    res.status(500).json({ error: 'Failed to fetch titles' });
  }
});

// Generate report endpoint
app.post('/api/generate-report', async (req, res) => {
  try {
    const { publisher, startDate, endDate, titles } = req.body;
    
    if (!titles || titles.length === 0) {
      return res.status(400).json({ error: 'No titles selected' });
    }

    // Create placeholders for SQL query
    const placeholders = titles.map((_, index) => `$${index + 4}`).join(',');
    
    const query = `
      SELECT 
        customer_name,
        country,
        city,
        COUNT(DISTINCT order_reference) as total_orders,
        SUM(quantity) as total_quantity,
        MAX(date) as last_order
      FROM records
      WHERE date >= $1 
        AND date <= $2
        AND title IN (${placeholders})
        AND customer_name NOT IN (
          SELECT customer_name FROM excluded_customers
        )
      GROUP BY customer_name, country, city
      ORDER BY country, city, customer_name
    `;
    
    const params = [startDate, endDate, publisher, ...titles];
    const result = await pool.query(query, params);
    
    res.json({
      success: true,
      data: result.rows,
      totalCustomers: result.rows.length,
      publisher: publisher
    });
    
  } catch (error) {
    console.error('Error generating report:', error);
    res.status(500).json({ error: 'Failed to generate report: ' + error.message });
  }
});

// Delete a record
app.delete('/records/:id', async (req, res) => {
  try {
    await pool.query('DELETE FROM records WHERE id = $1', [req.params.id]);
    res.json({ message: 'Record deleted successfully' });
  } catch (error) {
    console.error('Delete error:', error);
    res.status(500).json({ error: 'Failed to delete record' });
  }
});

// Start server
initDatabase().then(() => {
  app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
  });
});
