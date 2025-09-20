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
        country VARCHAR(100),
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

// Serve the landing page
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Serve the upload page
app.get('/upload', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'upload.html'));
});

// Serve the customers page
app.get('/customers', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'customers.html'));
});

// API endpoint to get customer data with statistics
app.get('/api/customers', async (req, res) => {
  try {
    // Get customer statistics
    const customerStats = await pool.query(`
      SELECT 
        customer_name,
        country,
        COUNT(*) as total_orders,
        SUM(quantity) as total_quantity,
        SUM(total) as total_revenue,
        MAX(order_date) as last_order
      FROM records 
      WHERE customer_name != 'Unknown'
      GROUP BY customer_name, country
      ORDER BY total_revenue DESC
    `);

    // Get overall statistics
    const overallStats = await pool.query(`
      SELECT 
        COUNT(DISTINCT customer_name) as total_customers,
        COUNT(DISTINCT country) as total_countries,
        COUNT(*) as total_orders,
        SUM(total) as total_revenue
      FROM records 
      WHERE customer_name != 'Unknown'
    `);

    res.json({
      customers: customerStats.rows,
      stats: overallStats.rows[0]
    });
  } catch (error) {
    console.error('Customer data error:', error);
    res.status(500).json({ error: 'Failed to fetch customer data' });
  }
});

// API endpoint to update customer information
app.post('/api/customers/update', async (req, res) => {
  try {
    const { customerName, field, value } = req.body;
    
    if (field === 'country') {
      await pool.query(
        'UPDATE records SET country = $1 WHERE customer_name = $2',
        [value, customerName]
      );
    }
    
    res.json({ success: true });
  } catch (error) {
    console.error('Customer update error:', error);
    res.status(500).json({ error: 'Failed to update customer' });
  }
});

// Upload and process Excel/CSV file
app.post('/upload', upload.single('excelFile'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    const dataType = req.body.dataType || 'gazelle'; // Default to gazelle format
    const isCSV = req.file.originalname.toLowerCase().endsWith('.csv');
    
    let data;
    
    if (isCSV) {
      // Parse CSV file
      const csvText = req.file.buffer.toString('utf8');
      const lines = csvText.split('\n');
      const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
      
      data = [];
      for (let i = 1; i < lines.length; i++) {
        if (lines[i].trim()) {
          const values = lines[i].split(',');
          const row = {};
          headers.forEach((header, index) => {
            row[header] = values[index] ? values[index].trim().replace(/"/g, '') : '';
          });
          data.push(row);
        }
      }
    } else {
      // Parse Excel file
      const workbook = XLSX.read(req.file.buffer, { type: 'buffer' });
      const sheetName = workbook.SheetNames[0];
      const worksheet = workbook.Sheets[sheetName];
      data = XLSX.utils.sheet_to_json(worksheet, { range: 1 });
    }
    
    console.log(`Processing ${dataType} data format (${isCSV ? 'CSV' : 'Excel'})`);
    console.log('Extracted data sample:', data.slice(0, 3));
    console.log('Column headers:', Object.keys(data[0] || {}));
    
    // Process and insert data
    const insertedRecords = [];
    const orderCustomerMap = new Map(); // Track customer names by order for Shopify
    
    // First pass for Shopify: collect customer names and countries by order
    if (dataType === 'shopify') {
      for (const row of data) {
        if (row['Name'] && (row['Billing Name'] || row['Billing Country'])) {
          if (row['Billing Name']) {
            orderCustomerMap.set(row['Name'], {
              name: row['Billing Name'],
              country: row['Billing Country'] || 'Unknown'
            });
          } else if (!orderCustomerMap.has(row['Name']) && row['Billing Country']) {
            // If we don't have name yet but have country, store what we have
            orderCustomerMap.set(row['Name'], {
              name: orderCustomerMap.get(row['Name'])?.name || 'Unknown',
              country: row['Billing Country']
            });
          }
        }
      }
    }
    
    for (const row of data) {
      // Skip empty rows
      if (!row || Object.keys(row).length === 0) continue;
      
      let record;
      
      if (dataType === 'gazelle') {
        // Gazelle format processing (existing logic)
        const keys = Object.keys(row);
        record = {
          order_date: parseDate(row[keys[0]] || new Date()),
          cus_no: row[keys[2]] || null,
          customer_name: row[keys[3]] || 'Unknown',
          title: row[keys[5]] || 'Unknown',
          book_ean: row[keys[7]] || null,
          quantity: parseInt(row[keys[8]]) || 0,
          total: parseFloat(row[keys[9]]) || 0,
          country: 'UK' // Default for Gazelle data
        };
      } else if (dataType === 'shopify') {
        // Shopify format processing based on your specifications
        const orderNumber = row['Name']; // Column A - Order number
        const orderData = orderCustomerMap.get(orderNumber) || { name: 'Unknown', country: 'Unknown' };
        const itemPrice = parseFloat(row['Lineitem price']) || 0; // Column S - price per item
        const quantity = parseInt(row['Lineitem quantity']) || 0; // Column Q - quantity
        const totalPrice = itemPrice * quantity; // Calculate total as price * quantity
        
        record = {
          order_date: parseDate(row['Created at']), // Column P - Date
          cus_no: null, // No customer number in Shopify data
          customer_name: orderData.name, // Column AC - Billing Name (from first order line)
          title: row['Lineitem name'] || 'Unknown', // Column R - Title
          book_ean: row['Lineitem sku'] || null, // SKU if available
          quantity: quantity, // Column Q - Quantity
          total: totalPrice, // Column S * Q - Price per item multiplied by quantity
          country: orderData.country // Column AG - Billing Country (from first order line)
        };
      }

      const result = await pool.query(
        'INSERT INTO records (order_date, cus_no, customer_name, title, book_ean, quantity, total, country) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING *',
        [record.order_date, record.cus_no, record.customer_name, record.title, record.book_ean, record.quantity, record.total, record.country]
      );
      
      insertedRecords.push(result.rows[0]);
    }

    // Log the file upload with data type
    await pool.query(
      'INSERT INTO upload_log (filename, records_count) VALUES ($1, $2)',
      [`${dataType.toUpperCase()}: ${req.file.originalname}`, insertedRecords.length]
    );

    res.json({ 
      message: `Successfully processed ${insertedRecords.length} ${dataType} records`, 
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
