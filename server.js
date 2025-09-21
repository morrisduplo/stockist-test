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

// Initialize database tables with enhanced tracking
async function initDatabase() {
  try {
    // Create records table with source tracking fields
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
        upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        order_reference VARCHAR(100),
        line_identifier TEXT,
        source_file VARCHAR(255),
        source_row INTEGER,
        upload_batch_id INTEGER,
        data_type VARCHAR(20)
      )
    `);
    
    // Add new columns if they don't exist (for existing databases)
    const newColumns = [
      { name: 'city', type: 'VARCHAR(100)' },
      { name: 'order_reference', type: 'VARCHAR(100)' },
      { name: 'line_identifier', type: 'TEXT' },
      { name: 'source_file', type: 'VARCHAR(255)' },
      { name: 'source_row', type: 'INTEGER' },
      { name: 'upload_batch_id', type: 'INTEGER' },
      { name: 'data_type', type: 'VARCHAR(20)' }
    ];
    
    for (const col of newColumns) {
      try {
        await pool.query(`ALTER TABLE records ADD COLUMN ${col.name} ${col.type}`);
        console.log(`Added ${col.name} column to records table`);
      } catch (err) {
        // Column already exists, that's fine
      }
    }
    
    // Create unique index for duplicate prevention
    try {
      await pool.query(`
        CREATE UNIQUE INDEX IF NOT EXISTS unique_order_line 
        ON records (order_reference, title, COALESCE(book_ean, ''), quantity, total)
      `);
      console.log('Created unique index for duplicate prevention');
    } catch (err) {
      // Index already exists
    }
    
    // Enhanced upload_log table with more details
    await pool.query(`
      CREATE TABLE IF NOT EXISTS upload_log (
        id SERIAL PRIMARY KEY,
        filename VARCHAR(255),
        records_count INTEGER,
        upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        data_type VARCHAR(20),
        unknown_customers INTEGER DEFAULT 0,
        duplicates_skipped INTEGER DEFAULT 0
      )
    `);
    
    // Add new columns to upload_log if they don't exist
    try {
      await pool.query('ALTER TABLE upload_log ADD COLUMN data_type VARCHAR(20)');
      await pool.query('ALTER TABLE upload_log ADD COLUMN unknown_customers INTEGER DEFAULT 0');
      await pool.query('ALTER TABLE upload_log ADD COLUMN duplicates_skipped INTEGER DEFAULT 0');
    } catch (err) {
      // Columns may already exist
    }
    
    // Create customer_exclusions table
    await pool.query(`
      CREATE TABLE IF NOT EXISTS customer_exclusions (
        id SERIAL PRIMARY KEY,
        customer_name VARCHAR(255) UNIQUE,
        excluded BOOLEAN DEFAULT false,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    console.log('Database initialized successfully with enhanced tracking');
  } catch (error) {
    console.error('Database initialization error:', error);
  }
}

// Utility function to parse dates
function parseDate(dateValue) {
  if (!dateValue) return new Date();
  if (dateValue instanceof Date) return dateValue;
  
  // Try to parse various date formats
  const parsed = new Date(dateValue);
  return isNaN(parsed.getTime()) ? new Date() : parsed;
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

// Function to clean customer names with special character handling
function cleanCustomerName(rawName, rowNumber, filename) {
  if (!rawName || typeof rawName !== 'string') {
    console.log(`Row ${rowNumber} in ${filename}: No customer name provided`);
    return null;
  }
  
  console.log(`Cleaning customer name from row ${rowNumber}: "${rawName}"`);
  
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
    console.log(`Row ${rowNumber}: Cleaned name: "${rawName}" -> "${cleaned}"`);
  }
  
  // Validation checks
  
  // Return null if the cleaned name is empty or too short
  if (cleaned.length < 2) {
    console.log(`Row ${rowNumber}: Rejected - too short: "${rawName}" -> "${cleaned}"`);
    return null;
  }
  
  // Return null if it looks like an address (starts with numbers)
  if (/^\d+\s/.test(cleaned)) {
    console.log(`Row ${rowNumber}: Rejected - looks like address: "${rawName}" -> "${cleaned}"`);
    return null;
  }
  
  // Return null if it's only special characters or numbers
  if (!/[a-zA-Z]/.test(cleaned)) {
    console.log(`Row ${rowNumber}: Rejected - no letters: "${rawName}" -> "${cleaned}"`);
    return null;
  }
  
  // Return null if it's too generic
  const genericNames = ['customer', 'guest', 'user', 'test', 'admin', 'default'];
  if (genericNames.includes(cleaned.toLowerCase())) {
    console.log(`Row ${rowNumber}: Rejected - generic name: "${rawName}" -> "${cleaned}"`);
    return null;
  }
  
  console.log(`Row ${rowNumber}: Accepted customer name: "${cleaned}"`);
  return cleaned;
}

// Function to create unique line identifier for duplicate detection
function createLineIdentifier(orderRef, title, ean, quantity, total) {
  const cleanTitle = (title || '').replace(/[^a-zA-Z0-9]/g, '').toLowerCase();
  const cleanEan = (ean || '').replace(/[^a-zA-Z0-9]/g, '');
  return `${orderRef}_${cleanTitle}_${cleanEan}_${quantity}_${total}`;
}

// Function to check if record already exists
async function recordExists(orderRef, title, ean, quantity, total) {
  try {
    const result = await pool.query(`
      SELECT id FROM records 
      WHERE order_reference = $1 
      AND title = $2 
      AND COALESCE(book_ean, '') = COALESCE($3, '')
      AND quantity = $4 
      AND total = $5
    `, [orderRef, title, ean || '', quantity, total]);
    
    return result.rows.length > 0;
  } catch (error) {
    console.error('Error checking if record exists:', error);
    return false;
  }
}

// Process Shopify data (CSV format) with source tracking
function processShopifyData(rawData, filename, uploadBatchId) {
  console.log('=== DEBUGGING CSV PROCESSING ===');
  console.log('File:', filename);
  console.log('Raw data length:', rawData.length);
  
  // Print all available columns
  const columns = Object.keys(rawData[0] || {});
  console.log('Available columns:', columns);
  
  // Step 1: Group data by order number (Name column - Column A)
  const orderGroups = new Map();
  
  rawData.forEach((row, index) => {
    const rowNumber = index + 2; // Excel rows start at 1, plus header row
    const orderNumber = row['Name']; // This is the unique order identifier (Column A)
    if (!orderNumber) {
      console.log(`Row ${rowNumber}: No order number, skipping`);
      return;
    }
    
    // Initialize order group if it doesn't exist
    if (!orderGroups.has(orderNumber)) {
      orderGroups.set(orderNumber, {
        customerInfo: {
          name: null,
          company: null,
          country: null,
          city: null,
          sourceRow: rowNumber
        },
        lineItems: []
      });
    }
    
    const order = orderGroups.get(orderNumber);
    
    // Try to extract customer information - prioritize company names for B2B
    if (!order.customerInfo.name && !order.customerInfo.company) {
      // Check for company fields first (better for B2B)
      if (row['Billing Company'] && row['Billing Company'].trim()) {
        const cleanCompany = cleanCustomerName(row['Billing Company'].trim(), rowNumber, filename);
        if (cleanCompany) {
          order.customerInfo.company = cleanCompany;
          order.customerInfo.name = cleanCompany;
        }
      } else if (row['Shipping Company'] && row['Shipping Company'].trim()) {
        const cleanCompany = cleanCustomerName(row['Shipping Company'].trim(), rowNumber, filename);
        if (cleanCompany) {
          order.customerInfo.company = cleanCompany;
          order.customerInfo.name = cleanCompany;
        }
      } else if (row['Billing Name'] && row['Billing Name'].trim()) {
        const cleanName = cleanCustomerName(row['Billing Name'].trim(), rowNumber, filename);
        if (cleanName) {
          order.customerInfo.name = cleanName;
        }
      } else if (row['Shipping Name'] && row['Shipping Name'].trim()) {
        const cleanName = cleanCustomerName(row['Shipping Name'].trim(), rowNumber, filename);
        if (cleanName) {
          order.customerInfo.name = cleanName;
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
    
    // Extract city information
    if (!order.customerInfo.city) {
      if (row['Billing City'] && row['Billing City'].trim()) {
        order.customerInfo.city = row['Billing City'].trim();
      } else if (row['Shipping City'] && row['Shipping City'].trim()) {
        order.customerInfo.city = row['Shipping City'].trim();
      }
    }
    
    // Add line item if it has product information
    if (row['Lineitem name'] && row['Lineitem quantity']) {
      row._sourceRow = rowNumber;
      order.lineItems.push(row);
    }
  });
  
  // Step 2: Create records for each line item with source tracking
  const processedRecords = [];
  let unknownCustomerCount = 0;
  
  orderGroups.forEach((order, orderNumber) => {
    const customerName = order.customerInfo.name || order.customerInfo.company || 'Unknown Customer';
    if (customerName === 'Unknown Customer') {
      unknownCustomerCount++;
      console.log(`Order ${orderNumber} from row ${order.customerInfo.sourceRow}: Creating Unknown Customer record`);
    }
    
    const country = order.customerInfo.country || 'Unknown';
    const city = order.customerInfo.city || 'Unknown';
    
    order.lineItems.forEach((item) => {
      const quantity = parseInt(item['Lineitem quantity']) || 0;
      const itemPrice = parseFloat(item['Lineitem price']) || 0;
      const totalPrice = itemPrice * quantity;
      const title = item['Lineitem name'] || 'Unknown Product';
      const ean = item['Lineitem sku'] || null;
      
      const record = {
        order_date: parseDate(item['Created at']),
        cus_no: null,
        customer_name: customerName,
        title: title,
        book_ean: ean,
        quantity: quantity,
        total: totalPrice,
        country: country,
        city: city,
        order_reference: orderNumber, // Store the unique order reference (Name column)
        line_identifier: createLineIdentifier(orderNumber, title, ean, quantity, totalPrice),
        source_file: filename,
        source_row: item._sourceRow,
        upload_batch_id: uploadBatchId,
        data_type: 'shopify'
      };
      
      processedRecords.push(record);
    });
  });
  
  console.log(`Created ${processedRecords.length} processed Shopify records`);
  console.log(`Found ${unknownCustomerCount} orders with Unknown Customer`);
  return { records: processedRecords, unknownCount: unknownCustomerCount };
}

// Process Gazelle data (Excel format) with source tracking
function processGazelleData(rawData, filename, uploadBatchId) {
  console.log('=== PROCESSING GAZELLE DATA ===');
  console.log('File:', filename);
  console.log('Raw data length:', rawData.length);
  
  const processedRecords = [];
  let unknownCustomerCount = 0;
  
  rawData.forEach((row, index) => {
    const rowNumber = index + 2; // Excel rows start at 1, plus header row
    
    try {
      // Skip empty rows
      if (!row || Object.keys(row).length === 0) return;
      
      // Gazelle format processing with duplicate prevention
      const keys = Object.keys(row);
      
      // Check if customer name (Column D, index 3) is missing
      const customerName = row[keys[3]];
      if (!customerName || customerName.trim() === '') {
        unknownCustomerCount++;
        console.log(`Row ${rowNumber}: No customer name in column D, defaulting to Unknown`);
      }
      
      // Column E contains the Invoice number (unique order reference)
      const invoiceNumber = row[keys[4]] || `INV_${index}_${Date.now()}`; // Column E (index 4)
      const title = row[keys[5]] || 'Unknown';
      const ean = row[keys[7]] || null;
      const quantity = parseInt(row[keys[8]]) || 0;
      const total = parseFloat(row[keys[9]]) || 0;
      
      const record = {
        order_date: parseDate(row[keys[0]] || new Date()),
        cus_no: row[keys[2]] || null,
        customer_name: customerName || 'Unknown',
        title: title,
        book_ean: ean,
        quantity: quantity,
        total: total,
        country: 'Unknown',
        city: 'Unknown',
        order_reference: invoiceNumber,
        line_identifier: createLineIdentifier(invoiceNumber, title, ean, quantity, total),
        source_file: filename,
        source_row: rowNumber,
        upload_batch_id: uploadBatchId,
        data_type: 'gazelle'
      };
      
      processedRecords.push(record);
    } catch (error) {
      console.error(`Error processing Gazelle row ${rowNumber}:`, error);
    }
  });
  
  console.log(`Created ${processedRecords.length} Gazelle records`);
  console.log(`Found ${unknownCustomerCount} rows with Unknown Customer`);
  return { records: processedRecords, unknownCount: unknownCustomerCount };
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

// Serve the reports page
app.get('/reports', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'reports.html'));
});

// NEW: API endpoint to search for Unknown customer sources
app.get('/api/unknown-sources', async (req, res) => {
  try {
    console.log('Fetching Unknown customer sources');
    
    const result = await pool.query(`
      SELECT 
        customer_name,
        source_file,
        source_row,
        data_type,
        COUNT(*) as record_count,
        MIN(order_date) as first_order,
        MAX(order_date) as last_order,
        SUM(total) as total_revenue,
        STRING_AGG(DISTINCT title, ', ' ORDER BY title) as sample_products
      FROM records
      WHERE customer_name = 'Unknown' 
         OR customer_name = 'Unknown Customer'
         OR customer_name IS NULL
      GROUP BY customer_name, source_file, source_row, data_type
      ORDER BY record_count DESC
    `);
    
    console.log(`Found ${result.rows.length} Unknown customer source groups`);
    res.json(result.rows);
    
  } catch (error) {
    console.error('Unknown sources API error:', error);
    res.status(500).json({ error: 'Failed to fetch unknown sources: ' + error.message });
  }
});

// NEW: API endpoint to get detailed records for editing
app.get('/api/records/unknown', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        id,
        order_date,
        customer_name,
        title,
        book_ean,
        quantity,
        total,
        country,
        city,
        source_file,
        source_row,
        data_type,
        order_reference
      FROM records
      WHERE customer_name = 'Unknown' 
         OR customer_name = 'Unknown Customer'
         OR customer_name IS NULL
      ORDER BY source_file, source_row
      LIMIT 100
    `);
    
    res.json(result.rows);
    
  } catch (error) {
    console.error('Fetch unknown records error:', error);
    res.status(500).json({ error: 'Failed to fetch unknown records: ' + error.message });
  }
});

// ENHANCED: API endpoint to update individual record with better validation
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
    
    // Get original record details for logging
    const originalRecord = await pool.query('SELECT * FROM records WHERE id = $1', [id]);
    
    if (originalRecord.rows.length === 0) {
      return res.status(404).json({ error: 'Record not found' });
    }
    
    const original = originalRecord.rows[0];
    console.log(`Original: customer="${original.customer_name}", source_file="${original.source_file}", row=${original.source_row}`);
    
    // Update the record
    const result = await pool.query(`
      UPDATE records 
      SET customer_name = $1, 
          country = $2, 
          city = $3, 
          title = $4, 
          upload_date = CURRENT_TIMESTAMP
      WHERE id = $5
      RETURNING *
    `, [customer_name.trim(), country || 'Unknown', city || 'Unknown', title ? title.trim() : original.title, id]);
    
    console.log(`Successfully updated record ${id} (originally from ${original.source_file} row ${original.source_row})`);
    res.json({ success: true, record: result.rows[0], original: original });
    
  } catch (error) {
    console.error('Update record error:', error);
    res.status(500).json({ error: 'Failed to update record: ' + error.message });
  }
});

// NEW: API endpoint to bulk update Unknown customers
app.post('/api/bulk-update-unknown', async (req, res) => {
  try {
    const { customer_name, source_file, source_row } = req.body;
    
    if (!customer_name || customer_name.trim() === '') {
      return res.status(400).json({ error: 'Customer name is required' });
    }
    
    console.log(`Bulk updating Unknown customers from ${source_file} row ${source_row}`);
    
    // Update all matching records
    const result = await pool.query(`
      UPDATE records 
      SET customer_name = $1
      WHERE source_file = $2 
      AND source_row = $3
      AND (customer_name = 'Unknown' OR customer_name = 'Unknown Customer' OR customer_name IS NULL)
      RETURNING id
    `, [customer_name.trim(), source_file, source_row]);
    
    console.log(`Updated ${result.rowCount} records`);
    res.json({ 
      success: true, 
      updated: result.rowCount,
      message: `Updated ${result.rowCount} records from ${source_file} row ${source_row}` 
    });
    
  } catch (error) {
    console.error('Bulk update unknown error:', error);
    res.status(500).json({ error: 'Failed to bulk update: ' + error.message });
  }
});

// API endpoint to get available titles for reports (excluding excluded customers)
app.get('/api/titles', async (req, res) => {
  try {
    console.log('Titles API endpoint called');
    
    // Get all unique titles from records, excluding those from excluded customers
    const result = await pool.query(`
      SELECT DISTINCT r.title
      FROM records r
      LEFT JOIN customer_exclusions ce ON r.customer_name = ce.customer_name
      WHERE r.title IS NOT NULL 
      AND r.title != '' 
      AND r.title != 'Unknown'
      AND COALESCE(ce.excluded, false) = false
      ORDER BY r.title
    `);
    
    const titles = result.rows.map(row => ({
      title: row.title,
      excluded: false
    }));
    
    console.log(`Found ${titles.length} available titles`);
    res.json(titles);
    
  } catch (error) {
    console.error('Titles API error:', error);
    res.status(500).json({ error: 'Failed to fetch titles: ' + error.message });
  }
});

// API endpoint to generate customer reports by titles
app.post('/api/generate-report', async (req, res) => {
  try {
    const { publisher, startDate, endDate, titles } = req.body;
    
    console.log('Generate report request:', { publisher, startDate, endDate, titlesCount: titles.length });
    
    // Validate inputs
    if (!publisher || !startDate || !endDate || !titles || titles.length === 0) {
      return res.status(400).json({ error: 'Missing required parameters' });
    }
    
    // Build the SQL query with excluded customers filter using PostgreSQL array
    const query = `
      SELECT 
        r.customer_name,
        r.country,
        r.city,
        COUNT(*) as total_orders,
        SUM(r.quantity) as total_quantity,
        SUM(r.total) as total_revenue,
        MAX(r.order_date) as last_order
      FROM records r
      LEFT JOIN customer_exclusions ce ON r.customer_name = ce.customer_name
      WHERE r.order_date >= $1 
      AND r.order_date <= $2
      AND r.title = ANY($3::text[])
      AND COALESCE(ce.excluded, false) = false
      GROUP BY r.customer_name, r.country, r.city
      ORDER BY total_revenue DESC
    `;
    
    const params = [startDate, endDate, titles];
    
    console.log('Executing query with params:', { startDate, endDate, titlesCount: titles.length });
    
    const result = await pool.query(query, params);
    
    console.log(`Report generated: ${result.rows.length} customers found`);
    
    res.json({
      data: result.rows,
      totalCustomers: result.rows.length,
      publisher: publisher,
      startDate: startDate,
      endDate: endDate,
      titles: titles
    });
    
  } catch (error) {
    console.error('Generate report error:', error);
    res.status(500).json({ error: 'Failed to generate report: ' + error.message });
  }
});

// API endpoint to get customer data with statistics including city
app.get('/api/customers', async (req, res) => {
  try {
    console.log('Customer API endpoint called');
    
    // Get customer statistics with exclusion status and city
    const customerStats = await pool.query(`
      SELECT 
        r.customer_name,
        r.country,
        r.city,
        COUNT(*) as total_orders,
        SUM(r.quantity) as total_quantity,
        SUM(r.total) as total_revenue,
        MAX(r.order_date) as last_order,
        COALESCE(ce.excluded, false) as excluded
      FROM records r
      LEFT JOIN customer_exclusions ce ON r.customer_name = ce.customer_name
      GROUP BY r.customer_name, r.country, r.city, ce.excluded
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

// API endpoint to update customer information including city
app.post('/api/customers/update', async (req, res) => {
  try {
    const { customerName, field, value } = req.body;
    
    if (field === 'country') {
      await pool.query(
        'UPDATE records SET country = $1 WHERE customer_name = $2',
        [value, customerName]
      );
    } else if (field === 'city') {
      await pool.query(
        'UPDATE records SET city = $1 WHERE customer_name = $2',
        [value, customerName]
      );
    }
    
    res.json({ success: true });
  } catch (error) {
    console.error('Customer update error:', error);
    res.status(500).json({ error: 'Failed to update customer' });
  }
});

// API endpoint to bulk update customer locations from CSV import
app.post('/api/customers/bulk-update', async (req, res) => {
  try {
    const { updates } = req.body;
    
    if (!updates || !Array.isArray(updates)) {
      return res.status(400).json({ error: 'Invalid update data' });
    }
    
    console.log(`Bulk update request for ${updates.length} customers`);
    
    let updatedCount = 0;
    const errors = [];
    
    // Start a transaction
    const client = await pool.connect();
    
    try {
      await client.query('BEGIN');
      
      for (const update of updates) {
        const { customerName, country, city } = update;
        
        if (!customerName) {
          errors.push(`Invalid customer name: ${customerName}`);
          continue;
        }
        
        // Update both country and city for this customer
        const updateQuery = `
          UPDATE records 
          SET country = $1, city = $2, upload_date = CURRENT_TIMESTAMP
          WHERE customer_name = $3
        `;
        
        const result = await client.query(updateQuery, [
          country || 'Unknown',
          city || 'Unknown', 
          customerName
        ]);
        
        if (result.rowCount > 0) {
          updatedCount++;
          console.log(`Updated ${result.rowCount} records for customer: ${customerName}`);
        } else {
          errors.push(`Customer not found: ${customerName}`);
        }
      }
      
      await client.query('COMMIT');
      
      console.log(`Bulk update completed: ${updatedCount} customers updated`);
      
      if (errors.length > 0) {
        console.warn('Bulk update errors:', errors);
      }
      
      res.json({ 
        success: true, 
        updated: updatedCount,
        total: updates.length,
        errors: errors 
      });
      
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
    
  } catch (error) {
    console.error('Bulk update error:', error);
    res.status(500).json({ error: 'Failed to bulk update customers: ' + error.message });
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

// Upload and process Excel/CSV file with enhanced tracking
app.post('/upload', upload.single('excelFile'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    const filename = req.file.originalname;
    const isCSV = filename.toLowerCase().endsWith('.csv');
    
    // Auto-detect data type based on file type and name
    let dataType;
    if (isCSV || filename.toLowerCase().includes('shopify')) {
      dataType = 'shopify';
    } else {
      dataType = 'gazelle'; // Excel files are typically Gazelle format
    }
    
    // Create upload batch ID
    const uploadBatchResult = await pool.query(
      'INSERT INTO upload_log (filename, records_count, data_type, unknown_customers, duplicates_skipped) VALUES ($1, $2, $3, $4, $5) RETURNING id',
      [filename, 0, dataType, 0, 0]
    );
    const uploadBatchId = uploadBatchResult.rows[0].id;
    
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
    console.log('File:', filename);
    console.log('Raw data length:', rawData.length);
    console.log('Column headers:', Object.keys(rawData[0] || {}));
    console.log('Sample row:', rawData[0]);
    
    // Process data based on detected type with source tracking
    let processedData;
    if (dataType === 'shopify') {
      processedData = processShopifyData(rawData, filename, uploadBatchId);
    } else {
      processedData = processGazelleData(rawData, filename, uploadBatchId);
    }
    
    const processedRecords = processedData.records;
    const unknownCustomerCount = processedData.unknownCount;
    
    // Insert into database with duplicate checking
    const insertedRecords = [];
    const skippedRecords = [];
    
    for (const record of processedRecords) {
      try {
        // Check if record already exists
        const exists = await recordExists(
          record.order_reference, 
          record.title, 
          record.book_ean, 
          record.quantity, 
          record.total
        );
        
        if (exists) {
          console.log(`Skipping duplicate: Order ${record.order_reference}, Row ${record.source_row}`);
          skippedRecords.push(record);
          continue;
        }
        
        // Insert the record with source tracking
        const result = await pool.query(
          `INSERT INTO records (
            order_date, cus_no, customer_name, title, book_ean, quantity, total, 
            country, city, order_reference, line_identifier,
            source_file, source_row, upload_batch_id, data_type
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15) 
          RETURNING *`,
          [
            record.order_date, record.cus_no, record.customer_name, record.title, 
            record.book_ean, record.quantity, record.total, record.country, record.city, 
            record.order_reference, record.line_identifier,
            record.source_file, record.source_row, record.upload_batch_id, record.data_type
          ]
        );
        
        insertedRecords.push(result.rows[0]);
      } catch (dbError) {
        if (dbError.code === '23505') { // Unique constraint violation
          console.log(`Duplicate detected: Row ${record.source_row}`);
          skippedRecords.push(record);
        } else {
          console.error('Database insert error for record:', record, dbError);
        }
      }
    }

    // Update upload log with final counts
    await pool.query(
      'UPDATE upload_log SET records_count = $1, unknown_customers = $2, duplicates_skipped = $3 WHERE id = $4',
      [insertedRecords.length, unknownCustomerCount, skippedRecords.length, uploadBatchId]
    );

    let message = `Successfully processed ${insertedRecords.length} ${dataType} records`;
    if (unknownCustomerCount > 0) {
      message += ` (${unknownCustomerCount} with Unknown customers)`;
    }
    if (skippedRecords.length > 0) {
      message += ` (${skippedRecords.length} duplicates skipped)`;
    }

    res.json({ 
      message: message,
      records: insertedRecords,
      skipped: skippedRecords.length,
      inserted: insertedRecords.length,
      unknownCustomers: unknownCustomerCount
    });

  } catch (error) {
    console.error('Upload error:', error);
    res.status(500).json({ error: 'Failed to process file: ' + error.message });
  }
});

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

// Start server
initDatabase().then(() => {
  app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
  });
});
