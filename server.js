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
    // Create tables if they don't exist (don't drop existing data)
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

// Clear all data endpoint
app.post('/clear-data', async (req, res) => {
  try {
    console.log('Clear data endpoint called');
    
    // Clear all records
    const recordsResult = await pool.query('DELETE FROM records');
    console.log('Records deleted:', recordsResult.rowCount);
    
    // Clear upload log
    const logResult = await pool.query('DELETE FROM upload_log');
    console.log('Upload log deleted:', logResult.rowCount);
    
    // Reset sequences (optional)
    await pool.query('ALTER SEQUENCE records_id_seq RESTART WITH 1');
    await pool.query('ALTER SEQUENCE upload_log_id_seq RESTART WITH 1');
    
    console.log('Data cleared successfully');
    res.json({ message: 'All data cleared successfully' });
  } catch (error) {
    console.error('Clear data error:', error);
    res.status(500).json({ error: 'Failed to clear data: ' + error.message });
  }
});

// Upload and process Excel/CSV file
app.post('/upload', upload.single('excelFile'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    const isCSV = req.file.originalname.toLowerCase().endsWith('.csv');
    
    // Auto-detect data type based on file type and name
    let dataType;
    if (isCSV || req.file.originalname.toLowerCase().includes('shopify')) {
      dataType = 'shopify';
    } else {
      dataType = 'gazelle'; // Excel files are typically Gazelle format
    }
    
    let rawData;
    
    if (isCSV) {
      // Parse CSV file with better handling
      const csvText = req.file.buffer.toString('utf8');
      const lines = csvText.split('\n').filter(line => line.trim());
      
      if (lines.length === 0) {
        return res.status(400).json({ error: 'CSV file is empty' });
      }
      
      // Parse headers - handle quoted headers
      const headerLine = lines[0];
      const headers = parseCSVLine(headerLine);
      
      rawData = [];
      for (let i = 1; i < lines.length; i++) {
        if (lines[i].trim()) {
          const values = parseCSVLine(lines[i]);
          const row = {};
          headers.forEach((header, index) => {
            row[header] = values[index] || '';
          });
          rawData.push(row);
        }
      }
    } else {
      // Parse Excel file
      const workbook = XLSX.read(req.file.buffer, { type: 'buffer' });
      const sheetName = workbook.SheetNames[0];
      const worksheet = workbook.Sheets[sheetName];
      rawData = XLSX.utils.sheet_to_json(worksheet, { range: 1 });
    }
    
    console.log(`Processing ${dataType} data format (${isCSV ? 'CSV' : 'Excel'})`);
    console.log('Raw data length:', rawData.length);
    console.log('Column headers:', Object.keys(rawData[0] || {}));
    console.log('Sample row:', rawData[0]);
    
    // Process data based on detected type
    let processedRecords;
    if (dataType === 'shopify') {
      processedRecords = processShopifyData(rawData);
    } else {
      processedRecords = processGazelleData(rawData);
    }
    
    // Insert into database
    const insertedRecords = [];
    for (const record of processedRecords) {
      try {
        const result = await pool.query(
          'INSERT INTO records (order_date, cus_no, customer_name, title, book_ean, quantity, total, country) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING *',
          [record.order_date, record.cus_no, record.customer_name, record.title, record.book_ean, record.quantity, record.total, record.country]
        );
        
        insertedRecords.push(result.rows[0]);
      } catch (dbError) {
        console.error('Database insert error for record:', record, dbError);
      }
    }

    // Log the file upload
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

// IMPROVED: Shopify data processing function with debugging
function processShopifyData(rawData) {
  console.log('=== DEBUGGING CSV PROCESSING ===');
  console.log('Raw data length:', rawData.length);
  
  // Print all available columns
  const columns = Object.keys(rawData[0] || {});
  console.log('Available columns:', columns);
  
  // Look for customer-related columns
  const customerColumns = columns.filter(col => 
    col.toLowerCase().includes('name') || 
    col.toLowerCase().includes('company') || 
    col.toLowerCase().includes('billing') ||
    col.toLowerCase().includes('shipping')
  );
  console.log('Customer-related columns:', customerColumns);
  
  // Show sample data for first few rows
  console.log('=== SAMPLE ROWS ===');
  rawData.slice(0, 3).forEach((row, i) => {
    console.log(`Row ${i + 1}:`);
    console.log('  Order Number (Name):', row['Name']);
    console.log('  Billing Name:', row['Billing Name']);
    console.log('  Shipping Name:', row['Shipping Name']);
    console.log('  Billing Company:', row['Billing Company']);
    console.log('  Shipping Company:', row['Shipping Company']);
    console.log('  Billing Country:', row['Billing Country']);
    console.log('  Shipping Country:', row['Shipping Country']);
    console.log('  Lineitem name:', row['Lineitem name']);
    console.log('  Lineitem quantity:', row['Lineitem quantity']);
    console.log('---');
  });
  
  // Step 1: Group data by order number
  const orderGroups = new Map();
  
  rawData.forEach((row, index) => {
    const orderNumber = row['Name'];
    if (!orderNumber) {
      console.log(`Row ${index + 1}: No order number, skipping`);
      return;
    }
    
    // Initialize order group if it doesn't exist
    if (!orderGroups.has(orderNumber)) {
      orderGroups.set(orderNumber, {
        customerInfo: {
          name: null,
          company: null,
          country: null
        },
        lineItems: []
      });
    }
    
    const order = orderGroups.get(orderNumber);
    
    // Try to extract customer information - prioritize company names for B2B
    if (!order.customerInfo.name && !order.customerInfo.company) {
      // Check for company fields first (better for B2B)
      if (row['Billing Company'] && row['Billing Company'].trim()) {
        order.customerInfo.company = row['Billing Company'].trim();
        order.customerInfo.name = row['Billing Company'].trim();
        console.log(`Found company "${order.customerInfo.company}" for order ${orderNumber}`);
      } else if (row['Shipping Company'] && row['Shipping Company'].trim()) {
        order.customerInfo.company = row['Shipping Company'].trim();
        order.customerInfo.name = row['Shipping Company'].trim();
        console.log(`Found shipping company "${order.customerInfo.company}" for order ${orderNumber}`);
      } else if (row['Billing Name'] && row['Billing Name'].trim()) {
        order.customerInfo.name = row['Billing Name'].trim();
        console.log(`Found billing name "${order.customerInfo.name}" for order ${orderNumber}`);
      } else if (row['Shipping Name'] && row['Shipping Name'].trim()) {
        order.customerInfo.name = row['Shipping Name'].trim();
        console.log(`Found shipping name "${order.customerInfo.name}" for order ${orderNumber}`);
      }
    }
    
    // Extract country information
    if (!order.customerInfo.country) {
      if (row['Billing Country'] && row['Billing Country'].trim()) {
        order.customerInfo.country = row['Billing Country'].trim();
      } else if (row['Shipping Country'] && row['Shipping Country'].trim()) {
        order.customerInfo.country = row['Shipping Country'].trim();
      }
    }
    
    // Add line item if it has product information
    if (row['Lineitem name'] && row['Lineitem quantity']) {
      order.lineItems.push(row);
    }
  });
  
  console.log(`=== ORDER SUMMARY ===`);
  console.log(`Processed ${orderGroups.size} unique orders`);
  
  // Debug each order
  let orderCount = 0;
  orderGroups.forEach((order, orderNumber) => {
    orderCount++;
    if (orderCount <= 5) { // Show first 5 orders for debugging
      console.log(`Order ${orderNumber}:`);
      console.log(`  Customer: "${order.customerInfo.name || 'NOT FOUND'}"`);
      console.log(`  Company: "${order.customerInfo.company || 'NOT FOUND'}"`);
      console.log(`  Country: "${order.customerInfo.country || 'NOT FOUND'}"`);
      console.log(`  Line Items: ${order.lineItems.length}`);
    }
  });
  
  // Step 2: Create records for each line item
  const processedRecords = [];
  
  orderGroups.forEach((order, orderNumber) => {
    const customerName = order.customerInfo.name || order.customerInfo.company || 'Unknown Customer';
    const country = order.customerInfo.country || 'Unknown';
    
    order.lineItems.forEach((item) => {
      const quantity = parseInt(item['Lineitem quantity']) || 0;
      const itemPrice = parseFloat(item['Lineitem price']) || 0;
      const totalPrice = itemPrice * quantity;
      
      const record = {
        order_date: parseDate(item['Created at']),
        cus_no: null,
        customer_name: customerName,
        title: item['Lineitem name'] || 'Unknown Product',
        book_ean: item['Lineitem sku'] || null,
        quantity: quantity,
        total: totalPrice,
        country: country
      };
      
      processedRecords.push(record);
    });
  });
  
  console.log(`=== FINAL RESULTS ===`);
  console.log(`Created ${processedRecords.length} processed records`);
  
  // Show first few processed records
  processedRecords.slice(0, 3).forEach((record, i) => {
    console.log(`Processed Record ${i + 1}:`);
    console.log(`  Customer: "${record.customer_name}"`);
    console.log(`  Country: "${record.country}"`);
    console.log(`  Product: "${record.title}"`);
    console.log(`  Quantity: ${record.quantity}`);
    console.log('---');
  });
  
  return processedRecords;
}

// Process Gazelle data (Excel format)
function processGazelleData(rawData) {
  console.log('=== PROCESSING GAZELLE DATA ===');
  console.log('Raw data length:', rawData.length);
  
  const processedRecords = [];
  
  rawData.forEach((row, index) => {
    try {
      // Skip empty rows
      if (!row || Object.keys(row).length === 0) return;
      
      // Gazelle format processing (existing logic)
      const keys = Object.keys(row);
      console.log(`Row ${index + 1} keys:`, keys.slice(0, 10)); // Show first 10 keys
      
      const record = {
        order_date: parseDate(row[keys[0]] || new Date()),
        cus_no: row[keys[2]] || null,
        customer_name: row[keys[3]] || 'Unknown',
        title: row[keys[5]] || 'Unknown',
        book_ean: row[keys[7]] || null,
        quantity: parseInt(row[keys[8]]) || 0,
        total: parseFloat(row[keys[9]]) || 0,
        country: 'UK' // Default for Gazelle data
      };
      
      // Debug output for first few records
      if (index < 3) {
        console.log(`Gazelle Record ${index + 1}:`);
        console.log(`  Date: ${record.order_date}`);
        console.log(`  Customer: ${record.customer_name}`);
        console.log(`  Title: ${record.title}`);
        console.log(`  Quantity: ${record.quantity}`);
        console.log('---');
      }
      
      processedRecords.push(record);
    } catch (error) {
      console.error(`Error processing Gazelle row ${index + 1}:`, error);
    }
  });
  
  console.log(`Created ${processedRecords.length} Gazelle records`);
  return processedRecords;
}

// Utility function to parse CSV lines (handles quoted values)
function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    
    if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === ',' && !inQuotes) {
      result.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }
  
  result.push(current.trim());
  return result;
}

// Get all records
app.get('/records', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM records ORDER BY upload_date DESC LIMIT 1000');
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
