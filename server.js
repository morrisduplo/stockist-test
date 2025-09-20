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

// Initialize database tables
async function initDatabase() {
  try {
    // Create records table with city field
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
        city VARCHAR(100),
        upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    // Add city column if it doesn't exist (for existing databases)
    try {
      await pool.query('ALTER TABLE records ADD COLUMN city VARCHAR(100)');
      console.log('Added city column to records table');
    } catch (err) {
      // Column probably already exists, ignore error
      console.log('City column already exists or error adding it:', err.message);
    }
    
    // Create upload_log table
    await pool.query(`
      CREATE TABLE IF NOT EXISTS upload_log (
        id SERIAL PRIMARY KEY,
        filename VARCHAR(255),
        records_count INTEGER,
        upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    // Create customer_exclusions table
    await pool.query(`
      CREATE TABLE IF NOT EXISTS customer_exclusions (
        id SERIAL PRIMARY KEY,
        customer_name VARCHAR(255) UNIQUE,
        excluded BOOLEAN DEFAULT false,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    console.log('Customer API endpoint called');
    
    // Get customer statistics with exclusion status
    const customerStats = await pool.query(`
      SELECT 
        r.customer_name,
        r.country,
        COUNT(*) as total_orders,
        SUM(r.quantity) as total_quantity,
        SUM(r.total) as total_revenue,
        MAX(r.order_date) as last_order,
        COALESCE(ce.excluded, false) as excluded
      FROM records r
      LEFT JOIN customer_exclusions ce ON r.customer_name = ce.customer_name
      GROUP BY r.customer_name, r.country, ce.excluded
      ORDER BY total_revenue DESC
    `);

    console.log(`Customer stats query returned ${customerStats.rows.length} customers`);

    // Get overall statistics
    const overallStats = await pool.query(`
      SELECT 
        COUNT(DISTINCT customer_name) as total_customers,
        COUNT(DISTINCT country) as total_countries,
        COUNT(*) as total_orders,
        SUM(total) as total_revenue
      FROM records
    `);

    console.log('Overall stats:', overallStats.rows[0]);

    res.json({
      customers: customerStats.rows,
      stats: overallStats.rows[0] || {
        total_customers: 0,
        total_countries: 0,
        total_orders: 0,
        total_revenue: 0
      }
    });
  } catch (error) {
    console.error('Customer API error:', error);
    res.status(500).json({ error: 'Failed to fetch customer data: ' + error.message });
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

// API endpoint to handle customer exclusions
app.post('/api/customers/exclude', async (req, res) => {
  try {
    const { customers, excluded } = req.body;
    
    console.log(`${excluded ? 'Excluding' : 'Including'} customers:`, customers);
    
    // Update exclusion status for each customer
    for (const customerName of customers) {
      await pool.query(`
        INSERT INTO customer_exclusions (customer_name, excluded, updated_at)
        VALUES ($1, $2, CURRENT_TIMESTAMP)
        ON CONFLICT (customer_name) 
        DO UPDATE SET excluded = $2, updated_at = CURRENT_TIMESTAMP
      `, [customerName, excluded]);
    }
    
    console.log(`Successfully ${excluded ? 'excluded' : 'included'} ${customers.length} customers`);
    res.json({ success: true, message: `${customers.length} customers updated` });
  } catch (error) {
    console.error('Customer exclusion error:', error);
    res.status(500).json({ error: 'Failed to update customer exclusions: ' + error.message });
  }
});

// API endpoint to update individual records
app.post('/api/update-record', async (req, res) => {
  try {
    const { id, customer_name, country, city, title } = req.body;
    
    if (!id) {
      return res.status(400).json({ error: 'Record ID is required' });
    }
    
    if (!customer_name || customer_name.trim() === '') {
      return res.status(400).json({ error: 'Customer name is required' });
    }
    
    console.log(`Updating record ${id}: customer="${customer_name}", country="${country}", city="${city}", title="${title}"`);
    
    // Update the record
    const result = await pool.query(`
      UPDATE records 
      SET customer_name = $1, country = $2, city = $3, title = $4, upload_date = CURRENT_TIMESTAMP
      WHERE id = $5
      RETURNING *
    `, [customer_name.trim(), country, city || 'London', title.trim(), id]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Record not found' });
    }
    
    console.log(`Successfully updated record ${id}`);
    res.json({ success: true, record: result.rows[0] });
    
  } catch (error) {
    console.error('Update record error:', error);
    res.status(500).json({ error: 'Failed to update record: ' + error.message });
  }
});

// Debug endpoint for specific record
app.get('/debug-record/:id', async (req, res) => {
  try {
    const recordId = req.params.id;
    
    // Get the specific record
    const record = await pool.query('SELECT * FROM records WHERE id = $1', [recordId]);
    
    if (record.rows.length === 0) {
      return res.json({ error: 'Record not found' });
    }
    
    // Get upload info
    const uploadInfo = await pool.query(`
      SELECT filename FROM upload_log 
      WHERE upload_date <= $1 
      ORDER BY upload_date DESC 
      LIMIT 1
    `, [record.rows[0].upload_date]);
    
    res.json({
      record: record.rows[0],
      uploadFile: uploadInfo.rows[0]?.filename || 'Unknown',
      debug: {
        customer_name: record.rows[0].customer_name,
        upload_date: record.rows[0].upload_date,
        country: record.rows[0].country,
        city: record.rows[0].city,
        title: record.rows[0].title
      }
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
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
    
    // Clear customer exclusions
    const exclusionsResult = await pool.query('DELETE FROM customer_exclusions');
    console.log('Customer exclusions deleted:', exclusionsResult.rowCount);
    
    // Reset sequences (optional)
    await pool.query('ALTER SEQUENCE records_id_seq RESTART WITH 1');
    await pool.query('ALTER SEQUENCE upload_log_id_seq RESTART WITH 1');
    await pool.query('ALTER SEQUENCE customer_exclusions_id_seq RESTART WITH 1');
    
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
          'INSERT INTO records (order_date, cus_no, customer_name, title, book_ean, quantity, total, country, city) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *',
          [record.order_date, record.cus_no, record.customer_name, record.title, record.book_ean, record.quantity, record.total, record.country, record.city]
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

// ENHANCED: Shopify data processing function with city support
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
    col.toLowerCase().includes('shipping') ||
    col.toLowerCase().includes('city')
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
    console.log('  Billing City:', row['Billing City']);
    console.log('  Shipping City:', row['Shipping City']);
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
          country: null,
          city: null
        },
        lineItems: []
      });
    }
    
    const order = orderGroups.get(orderNumber);
    
    // Try to extract customer information - prioritize company names for B2B
    if (!order.customerInfo.name && !order.customerInfo.company) {
      // Check for company fields first (better for B2B)
      if (row['Billing Company'] && row['Billing Company'].trim()) {
        const cleanCompany = cleanCustomerName(row['Billing Company'].trim());
        if (cleanCompany) {
          order.customerInfo.company = cleanCompany;
          order.customerInfo.name = cleanCompany;
          console.log(`Found company "${cleanCompany}" for order ${orderNumber} (original: "${row['Billing Company']}")`);
        }
      } else if (row['Shipping Company'] && row['Shipping Company'].trim()) {
        const cleanCompany = cleanCustomerName(row['Shipping Company'].trim());
        if (cleanCompany) {
          order.customerInfo.company = cleanCompany;
          order.customerInfo.name = cleanCompany;
          console.log(`Found shipping company "${cleanCompany}" for order ${orderNumber} (original: "${row['Shipping Company']}")`);
        }
      } else if (row['Billing Name'] && row['Billing Name'].trim()) {
        const cleanName = cleanCustomerName(row['Billing Name'].trim());
        if (cleanName) {
          order.customerInfo.name = cleanName;
          console.log(`Found billing name "${cleanName}" for order ${orderNumber} (original: "${row['Billing Name']}")`);
        }
      } else if (row['Shipping Name'] && row['Shipping Name'].trim()) {
        const cleanName = cleanCustomerName(row['Shipping Name'].trim());
        if (cleanName) {
          order.customerInfo.name = cleanName;
          console.log(`Found shipping name "${cleanName}" for order ${orderNumber} (original: "${row['Shipping Name']}")`);
        }
      }
    }
    
    // Extract country and city information
    if (!order.customerInfo.country) {
      if (row['Billing Country'] && row['Billing Country'].trim()) {
        order.customerInfo.country = row['Billing Country'].trim();
      } else if (row['Shipping Country'] && row['Shipping Country'].trim()) {
        order.customerInfo.country = row['Shipping Country'].trim();
      }
    }
    
    // Extract city information from column AD (Billing City)
    if (!order.customerInfo.city) {
      if (row['Billing City'] && row['Billing City'].trim()) {
        order.customerInfo.city = row['Billing City'].trim();
        console.log(`Found city "${order.customerInfo.city}" for order ${orderNumber}`);
      } else if (row['Shipping City'] && row['Shipping City'].trim()) {
        order.customerInfo.city = row['Shipping City'].trim();
        console.log(`Found shipping city "${order.customerInfo.city}" for order ${orderNumber}`);
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
      console.log(`  City: "${order.customerInfo.city || 'NOT FOUND'}"`);
      console.log(`  Line Items: ${order.lineItems.length}`);
    }
  });
  
  // Step 2: Create records for each line item
  const processedRecords = [];
  
  orderGroups.forEach((order, orderNumber) => {
    const customerName = order.customerInfo.name || order.customerInfo.company || 'Unknown Customer';
    const country = order.customerInfo.country || 'Unknown';
    const city = order.customerInfo.city || 'Unknown'; // For CSV files
    
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
        country: country,
        city: city
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
    console.log(`  City: "${record.city}"`);
    console.log(`  Product: "${record.title}"`);
    console.log(`  Quantity: ${record.quantity}`);
    console.log('---');
  });
  
  return processedRecords;
}

// ENHANCED: Function to clean customer names with special character handling
function cleanCustomerName(rawName) {
  if (!rawName || typeof rawName !== 'string') {
    return null;
  }
  
  console.log(`Cleaning customer name: "${rawName}"`);
  
  // Remove leading/trailing whitespace
  let cleaned = rawName.trim();
  
  // Handle special characters and encoding issues
  cleaned = cleaned
    // Remove or replace HTML entities
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&#\d+;/g, '') // Remove numeric HTML entities
    
    // Normalize quotes and apostrophes
    .replace(/[""]/g, '"')
    .replace(/['']/g, "'")
    
    // Remove control characters and weird whitespace
    .replace(/[\x00-\x1F\x7F-\x9F]/g, '')
    .replace(/[\u2000-\u200B\u2028\u2029\u202F\u205F\u3000]/g, ' ')
    
    // Replace multiple spaces with single space
    .replace(/\s+/g, ' ')
    
    // Final trim
    .trim();
  
  // If name contains a comma, take only the part before the first comma
  if (cleaned.includes(',')) {
    const beforeComma = cleaned.split(',')[0].trim();
    console.log(`Split on comma: "${cleaned}" -> "${beforeComma}"`);
    cleaned = beforeComma;
  }
  
  // Additional cleanup - remove common problematic patterns
  cleaned = cleaned
    // Remove common address indicators
    .replace(/^(c\/o|care of|attn:|attention:)\s+/i, '')
    // Remove leading numbers that might be addresses
    .replace(/^\d+[a-zA-Z]?\s+/, '')
    // Remove trailing address-like patterns
    .replace(/\s+(ltd|limited|inc|corp|gmbh|srl|bv)\.?$/i, (match) => match) // Keep these
    .replace(/\s+\d+\s*$/, '') // Remove trailing numbers
    .trim();
  
  // Log what we're doing
  if (cleaned !== rawName.trim()) {
    console.log(`Cleaned name: "${rawName}" -> "${cleaned}"`);
  }
  
  // Validation checks
  
  // Return null if the cleaned name is empty or too short
  if (cleaned.length < 2) {
    console.log(`Rejected - too short: "${rawName}" -> "${cleaned}"`);
    return null;
  }
  
  // Return null if it looks like an address (starts with numbers)
  if (/^\d+\s/.test(cleaned)) {
    console.log(`Rejected - looks like address: "${rawName}" -> "${cleaned}"`);
    return null;
  }
  
  // Return null if it's only special characters or numbers
  if (!/[a-zA-Z]/.test(cleaned)) {
    console.log(`Rejected - no letters: "${rawName}" -> "${cleaned}"`);
    return null;
  }
  
  // Return null if it's too generic
  const genericNames = ['customer', 'guest', 'user', 'test', 'admin', 'default'];
  if (genericNames.includes(cleaned.toLowerCase())) {
    console.log(`Rejected - generic name: "${rawName}" -> "${cleaned}"`);
    return null;
  }
  
  console.log(`Accepted customer name: "${cleaned}"`);
  return cleaned;
}

// Process Gazelle data (Excel format) with London default city
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
        country: 'UK', // Default for Gazelle data
        city: 'London' // Default for Excel files (no city data available)
      };
      
      // Debug output for first few records
      if (index < 3) {
        console.log(`Gazelle Record ${index + 1}:`);
        console.log(`  Date: ${record.order_date}`);
        console.log(`  Customer: ${record.customer_name}`);
        console.log(`  City: ${record.city}`);
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

// Get records with pagination (500 per page)
app.get('/records', async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = 500; // Fixed at 500 records per page
    const offset = (page - 1) * limit;
    
    console.log(`Fetching page ${page}, offset ${offset}, limit ${limit}`);
    
    // Get total count
    const countResult = await pool.query('SELECT COUNT(*) FROM records');
    const totalRecords = parseInt(countResult.rows[0].count);
    
    // Get paginated records
    const result = await pool.query(
      'SELECT * FROM records ORDER BY upload_date DESC LIMIT $1 OFFSET $2', 
      [limit, offset]
    );
    
    const totalPages = Math.ceil(totalRecords / limit);
    
    console.log(`Returning ${result.rows.length} records, page ${page} of ${totalPages}`);
    
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
