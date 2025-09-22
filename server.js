const express = require('express');
const multer = require('multer');
const XLSX = require('xlsx');
const { Pool } = require('pg');
const path = require('path');
const bcrypt = require('bcrypt');
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

// ============================================
// CUSTOMER NAME MAPPING CONFIGURATION
// ============================================
// Add your customer name mappings here
// The system will automatically replace any customer name on the left with the name on the right
const CUSTOMER_NAME_MAPPINGS = {
  'ANTENNE - DIRECT UK': 'Antenne Online UK',
  'ANTENNE - EXPORT': 'Antenne Online UK',
  // Add more mappings as needed in the future:
  // 'OLD NAME': 'NEW NAME',
  // 'ANOTHER OLD NAME': 'ANOTHER NEW NAME',
};

// Function to apply customer name mapping
function applyCustomerNameMapping(customerName) {
  if (!customerName) return customerName;
  
  // Check if this customer name should be mapped to another name
  const upperCustomerName = customerName.toUpperCase().trim();
  
  for (const [oldName, newName] of Object.entries(CUSTOMER_NAME_MAPPINGS)) {
    if (upperCustomerName === oldName.toUpperCase()) {
      console.log(`Mapping customer name: "${customerName}" -> "${newName}"`);
      return newName;
    }
  }
  
  return customerName; // Return original if no mapping found
}

// Initialize database tables
async function initDatabase() {
  try {
    // Create records table with additional fields for duplicate detection
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
        line_identifier TEXT
      )
    `);
    
    // Add new columns if they don't exist (for existing databases)
    try {
      await pool.query('ALTER TABLE records ADD COLUMN city VARCHAR(100)');
      console.log('Added city column to records table');
    } catch (err) {
      console.log('City column already exists or error adding it:', err.message);
    }
    
    try {
      await pool.query('ALTER TABLE records ADD COLUMN order_reference VARCHAR(100)');
      console.log('Added order_reference column to records table');
    } catch (err) {
      console.log('Order_reference column already exists or error adding it:', err.message);
    }
    
    try {
      await pool.query('ALTER TABLE records ADD COLUMN line_identifier TEXT');
      console.log('Added line_identifier column to records table');
    } catch (err) {
      console.log('Line_identifier column already exists or error adding it:', err.message);
    }
    
    // Create unique index for duplicate prevention
    try {
      await pool.query(`
        CREATE UNIQUE INDEX IF NOT EXISTS unique_order_line 
        ON records (order_reference, title, COALESCE(book_ean, ''), quantity, total)
      `);
      console.log('Created unique index for duplicate prevention');
    } catch (err) {
      console.log('Unique index already exists or error creating it:', err.message);
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
    
    // Create users table
    await pool.query(`
      CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        username VARCHAR(100) UNIQUE NOT NULL,
        email VARCHAR(255),
        password_hash VARCHAR(255) NOT NULL,
        role VARCHAR(50) DEFAULT 'viewer',
        active BOOLEAN DEFAULT true,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    // Create default admin user if no users exist
    try {
      const userCount = await pool.query('SELECT COUNT(*) FROM users');
      if (parseInt(userCount.rows[0].count) === 0) {
        const defaultPassword = await bcrypt.hash('admin123', 10);
        await pool.query(
          'INSERT INTO users (username, email, password_hash, role, active) VALUES ($1, $2, $3, $4, $5)',
          ['admin', 'admin@antennebooks.com', defaultPassword, 'admin', true]
        );
        console.log('Default admin user created (username: admin, password: admin123)');
      }
    } catch (bcryptError) {
      console.log('Error creating default user:', bcryptError.message);
    }
    
    console.log('Database initialized successfully');
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
  
  // APPLY CUSTOMER NAME MAPPING HERE
  cleaned = applyCustomerNameMapping(cleaned);
  
  console.log(`Accepted customer name: "${cleaned}"`);
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

// Process Shopify data (CSV format) with duplicate prevention and customer mapping
function processShopifyData(rawData) {
  console.log('=== DEBUGGING CSV PROCESSING ===');
  console.log('Raw data length:', rawData.length);
  
  // Print all available columns
  const columns = Object.keys(rawData[0] || {});
  console.log('Available columns:', columns);
  
  // Step 1: Group data by order number (Name column - Column A)
  const orderGroups = new Map();
  
  rawData.forEach((row, index) => {
    const orderNumber = row['Name']; // This is the unique order identifier (Column A)
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
        }
      } else if (row['Shipping Company'] && row['Shipping Company'].trim()) {
        const cleanCompany = cleanCustomerName(row['Shipping Company'].trim());
        if (cleanCompany) {
          order.customerInfo.company = cleanCompany;
          order.customerInfo.name = cleanCompany;
        }
      } else if (row['Billing Name'] && row['Billing Name'].trim()) {
        const cleanName = cleanCustomerName(row['Billing Name'].trim());
        if (cleanName) {
          order.customerInfo.name = cleanName;
        }
      } else if (row['Shipping Name'] && row['Shipping Name'].trim()) {
        const cleanName = cleanCustomerName(row['Shipping Name'].trim());
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
      order.lineItems.push(row);
    }
  });
  
  // Step 2: Create records for each line item
  const processedRecords = [];
  
  orderGroups.forEach((order, orderNumber) => {
    const customerName = order.customerInfo.name || order.customerInfo.company || 'Unknown Customer';
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
        line_identifier: createLineIdentifier(orderNumber, title, ean, quantity, totalPrice)
      };
      
      processedRecords.push(record);
    });
  });
  
  console.log(`Created ${processedRecords.length} processed Shopify records`);
  return processedRecords;
}

// Process Gazelle data (Excel format) with Unknown defaults, duplicate prevention, and customer mapping
function processGazelleData(rawData) {
  console.log('=== PROCESSING GAZELLE DATA ===');
  console.log('Raw data length:', rawData.length);
  
  const processedRecords = [];
  let mappedCustomerCount = 0;
  
  rawData.forEach((row, index) => {
    try {
      // Skip empty rows
      if (!row || Object.keys(row).length === 0) return;
      
      // Gazelle format processing with duplicate prevention
      const keys = Object.keys(row);
      
      // Get customer name and apply mapping
      let customerName = row[keys[3]] || 'Unknown'; // Column D (index 3)
      
      // Apply customer name mapping for Gazelle data
      const originalName = customerName;
      customerName = applyCustomerNameMapping(customerName);
      
      if (originalName !== customerName) {
        mappedCustomerCount++;
        console.log(`Row ${index + 1}: Mapped customer "${originalName}" to "${customerName}"`);
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
        customer_name: customerName,
        title: title,
        book_ean: ean,
        quantity: quantity,
        total: total,
        country: 'Unknown', // Changed from 'UK' to 'Unknown'
        city: 'Unknown', // Changed from 'London' to 'Unknown'
        order_reference: invoiceNumber, // Store the invoice number as order reference
        line_identifier: createLineIdentifier(invoiceNumber, title, ean, quantity, total)
      };
      
      processedRecords.push(record);
    } catch (error) {
      console.error(`Error processing Gazelle row ${index + 1}:`, error);
    }
  });
  
  if (mappedCustomerCount > 0) {
    console.log(`Total customer names mapped: ${mappedCustomerCount}`);
  }
  
  console.log(`Created ${processedRecords.length} Gazelle records`);
  return processedRecords;
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

// Serve the settings page
app.get('/settings', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'settings.html'));
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
    
    // Update the record - use 'Unknown' as default for city
    const result = await pool.query(`
      UPDATE records 
      SET customer_name = $1, country = $2, city = $3, title = $4, upload_date = CURRENT_TIMESTAMP
      WHERE id = $5
      RETURNING *
    `, [customer_name.trim(), country, city || 'Unknown', title.trim(), id]);
    
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

// Upload and process Excel/CSV file with duplicate prevention
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
          console.log(`Skipping duplicate record: Order ${record.order_reference}, Product: ${record.title}`);
          skippedRecords.push(record);
          continue;
        }
        
        // Insert the record
        const result = await pool.query(
          'INSERT INTO records (order_date, cus_no, customer_name, title, book_ean, quantity, total, country, city, order_reference, line_identifier) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING *',
          [record.order_date, record.cus_no, record.customer_name, record.title, record.book_ean, record.quantity, record.total, record.country, record.city, record.order_reference, record.line_identifier]
        );
        
        insertedRecords.push(result.rows[0]);
      } catch (dbError) {
        if (dbError.code === '23505') { // Unique constraint violation
          console.log(`Duplicate record detected and skipped: ${record.order_reference}`);
          skippedRecords.push(record);
        } else {
          console.error('Database insert error for record:', record, dbError);
        }
      }
    }

    // Log the file upload
    await pool.query(
      'INSERT INTO upload_log (filename, records_count) VALUES ($1, $2)',
      [`${dataType.toUpperCase()}: ${req.file.originalname}`, insertedRecords.length]
    );

    let message = `Successfully processed ${insertedRecords.length} ${dataType} records`;
    if (skippedRecords.length > 0) {
      message += ` (${skippedRecords.length} duplicates skipped)`;
    }

    res.json({ 
      message: message,
      records: insertedRecords,
      skipped: skippedRecords.length,
      inserted: insertedRecords.length
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

// ============================================
// SETTINGS PAGE ROUTES
// ============================================

// Store settings in memory (in production, you might want to use a database table)
let appSettings = {
  general: {
    autoDetect: true,
    skipDuplicates: true,
    defaultCountry: 'Unknown',
    defaultCity: 'Unknown'
  },
  advanced: {
    recordsPerPage: 500,
    debugLogging: false,
    dbTimeout: 30
  }
};

// Get customer name mappings
app.get('/api/settings/mappings', (req, res) => {
  const mappings = Object.entries(CUSTOMER_NAME_MAPPINGS).map(([oldName, newName]) => ({
    oldName,
    newName
  }));
  res.json(mappings);
});

// Add new customer name mapping
app.post('/api/settings/mappings', (req, res) => {
  const { oldName, newName } = req.body;
  
  if (!oldName || !newName) {
    return res.status(400).json({ error: 'Both old and new names are required' });
  }
  
  CUSTOMER_NAME_MAPPINGS[oldName] = newName;
  
  const mappings = Object.entries(CUSTOMER_NAME_MAPPINGS).map(([oldName, newName]) => ({
    oldName,
    newName
  }));
  
  console.log(`Added mapping: "${oldName}" -> "${newName}"`);
  res.json({ success: true, mappings });
});

// Remove customer name mapping
app.delete('/api/settings/mappings/:index', (req, res) => {
  const index = parseInt(req.params.index);
  const entries = Object.entries(CUSTOMER_NAME_MAPPINGS);
  
  if (index >= 0 && index < entries.length) {
    const [oldName] = entries[index];
    delete CUSTOMER_NAME_MAPPINGS[oldName];
    
    console.log(`Removed mapping for: "${oldName}"`);
  }
  
  const mappings = Object.entries(CUSTOMER_NAME_MAPPINGS).map(([oldName, newName]) => ({
    oldName,
    newName
  }));
  
  res.json({ success: true, mappings });
});

// Import multiple mappings
app.post('/api/settings/mappings/import', (req, res) => {
  const { mappings } = req.body;
  
  if (!mappings || !Array.isArray(mappings)) {
    return res.status(400).json({ error: 'Invalid mappings data' });
  }
  
  // Add all mappings
  mappings.forEach(({ oldName, newName }) => {
    if (oldName && newName) {
      CUSTOMER_NAME_MAPPINGS[oldName] = newName;
    }
  });
  
  const updatedMappings = Object.entries(CUSTOMER_NAME_MAPPINGS).map(([oldName, newName]) => ({
    oldName,
    newName
  }));
  
  console.log(`Imported ${mappings.length} mappings`);
  res.json({ success: true, mappings: updatedMappings });
});

// Get statistics for settings page
app.get('/api/settings/statistics', async (req, res) => {
  try {
    const recordsCount = await pool.query('SELECT COUNT(*) FROM records');
    const customersCount = await pool.query('SELECT COUNT(DISTINCT customer_name) FROM records');
    const uploadsCount = await pool.query('SELECT COUNT(*) FROM upload_log');
    const excludedCount = await pool.query('SELECT COUNT(*) FROM customer_exclusions WHERE excluded = true');
    
    res.json({
      totalRecords: parseInt(recordsCount.rows[0].count),
      totalCustomers: parseInt(customersCount.rows[0].count),
      totalUploads: parseInt(uploadsCount.rows[0].count),
      excludedCustomers: parseInt(excludedCount.rows[0].count)
    });
  } catch (error) {
    console.error('Error fetching statistics:', error);
    res.status(500).json({ error: 'Failed to fetch statistics' });
  }
});

// Get general settings
app.get('/api/settings/general', (req, res) => {
  res.json(appSettings.general);
});

// Save general settings
app.post('/api/settings/general', (req, res) => {
  const { autoDetect, skipDuplicates, defaultCountry, defaultCity } = req.body;
  
  appSettings.general = {
    autoDetect: autoDetect !== undefined ? autoDetect : appSettings.general.autoDetect,
    skipDuplicates: skipDuplicates !== undefined ? skipDuplicates : appSettings.general.skipDuplicates,
    defaultCountry: defaultCountry || appSettings.general.defaultCountry,
    defaultCity: defaultCity || appSettings.general.defaultCity
  };
  
  console.log('Updated general settings:', appSettings.general);
  res.json({ success: true, settings: appSettings.general });
});

// Get advanced settings
app.get('/api/settings/advanced', (req, res) => {
  res.json(appSettings.advanced);
});

// Save advanced settings
app.post('/api/settings/advanced', (req, res) => {
  const { recordsPerPage, debugLogging, dbTimeout } = req.body;
  
  appSettings.advanced = {
    recordsPerPage: parseInt(recordsPerPage) || appSettings.advanced.recordsPerPage,
    debugLogging: debugLogging !== undefined ? debugLogging : appSettings.advanced.debugLogging,
    dbTimeout: parseInt(dbTimeout) || appSettings.advanced.dbTimeout
  };
  
  console.log('Updated advanced settings:', appSettings.advanced);
  res.json({ success: true, settings: appSettings.advanced });
});

// Backup database
app.get('/api/backup', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT * FROM records 
      ORDER BY upload_date DESC
    `);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'No data to backup' });
    }
    
    // Create CSV content
    const headers = Object.keys(result.rows[0]);
    const csv = [
      headers.join(','),
      ...result.rows.map(row => 
        headers.map(header => {
          const value = row[header];
          // Handle null values and quotes
          if (value === null || value === undefined) return '';
          // Escape quotes and wrap in quotes if contains comma
          const strValue = value.toString();
          if (strValue.includes(',') || strValue.includes('"')) {
            return `"${strValue.replace(/"/g, '""')}"`;
          }
          return strValue;
        }).join(',')
      )
    ].join('\n');
    
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="backup_${new Date().toISOString().split('T')[0]}.csv"`);
    res.send(csv);
    
  } catch (error) {
    console.error('Backup error:', error);
    res.status(500).json({ error: 'Failed to create backup' });
  }
});

// Export customers
app.get('/api/export/customers', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        customer_name,
        country,
        city,
        COUNT(*) as total_orders,
        SUM(quantity) as total_quantity,
        SUM(total) as total_revenue,
        MAX(order_date) as last_order
      FROM records
      GROUP BY customer_name, country, city
      ORDER BY customer_name
    `);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'No customers to export' });
    }
    
    // Create CSV content
    const csv = [
      'Customer Name,Country,City,Total Orders,Total Quantity,Total Revenue,Last Order',
      ...result.rows.map(row => 
        [
          `"${row.customer_name || ''}"`,
          `"${row.country || ''}"`,
          `"${row.city || ''}"`,
          row.total_orders || 0,
          row.total_quantity || 0,
          row.total_revenue || 0,
          row.last_order ? new Date(row.last_order).toISOString().split('T')[0] : ''
        ].join(',')
      )
    ].join('\n');
    
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="customers_${new Date().toISOString().split('T')[0]}.csv"`);
    res.send(csv);
    
  } catch (error) {
    console.error('Export customers error:', error);
    res.status(500).json({ error: 'Failed to export customers' });
  }
});

// Clear all records (danger zone)
app.post('/api/settings/clear-records', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM records');
    console.log(`Cleared ${result.rowCount} records from database`);
    
    // Reset sequence
    await pool.query('ALTER SEQUENCE records_id_seq RESTART WITH 1');
    
    res.json({ success: true, message: `Cleared ${result.rowCount} records` });
  } catch (error) {
    console.error('Clear records error:', error);
    res.status(500).json({ error: 'Failed to clear records' });
  }
});

// Reset all exclusions
app.post('/api/settings/reset-exclusions', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM customer_exclusions');
    console.log(`Reset ${result.rowCount} customer exclusions`);
    
    // Reset sequence
    await pool.query('ALTER SEQUENCE customer_exclusions_id_seq RESTART WITH 1');
    
    res.json({ success: true, message: `Reset ${result.rowCount} exclusions` });
  } catch (error) {
    console.error('Reset exclusions error:', error);
    res.status(500).json({ error: 'Failed to reset exclusions' });
  }
});

// Clear upload history
app.post('/api/settings/clear-upload-history', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM upload_log');
    console.log(`Cleared ${result.rowCount} upload history records`);
    
    // Reset sequence
    await pool.query('ALTER SEQUENCE upload_log_id_seq RESTART WITH 1');
    
    res.json({ success: true, message: `Cleared ${result.rowCount} upload records` });
  } catch (error) {
    console.error('Clear upload history error:', error);
    res.status(500).json({ error: 'Failed to clear upload history' });
  }
});

// ============================================
// USER MANAGEMENT ROUTES
// ============================================

// Get all users
app.get('/api/users', async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT id, username, email, role, active, created_at FROM users ORDER BY id'
    );
    res.json(result.rows);
  } catch (error) {
    console.error('Error fetching users:', error);
    res.status(500).json({ error: 'Failed to fetch users' });
  }
});

// Get single user
app.get('/api/users/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const result = await pool.query(
      'SELECT id, username, email, role, active, created_at FROM users WHERE id = $1',
      [id]
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }
    
    res.json(result.rows[0]);
  } catch (error) {
    console.error('Error fetching user:', error);
    res.status(500).json({ error: 'Failed to fetch user' });
  }
});

// Create new user
app.post('/api/users', async (req, res) => {
  try {
    const { username, email, password, role, active } = req.body;
    
    // Validate required fields
    if (!username || !password) {
      return res.status(400).json({ error: 'Username and password are required' });
    }
    
    // Check if username already exists
    const existingUser = await pool.query(
      'SELECT id FROM users WHERE username = $1',
      [username]
    );
    
    if (existingUser.rows.length > 0) {
      return res.status(400).json({ error: 'Username already exists' });
    }
    
    // Hash password
    const passwordHash = await bcrypt.hash(password, 10);
    
    // Insert new user
    const result = await pool.query(
      'INSERT INTO users (username, email, password_hash, role, active) VALUES ($1, $2, $3, $4, $5) RETURNING id, username, email, role, active, created_at',
      [username, email || null, passwordHash, role || 'viewer', active !== false]
    );
    
    console.log(`User created: ${username}`);
    res.json(result.rows[0]);
  } catch (error) {
    console.error('Error creating user:', error);
    res.status(500).json({ error: 'Failed to create user' });
  }
});

// Update user
app.put('/api/users/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { username, email, password, role, active } = req.body;
    
    // Check if user exists
    const existingUser = await pool.query(
      'SELECT id FROM users WHERE id = $1',
      [id]
    );
    
    if (existingUser.rows.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }
    
    // Check if new username conflicts with another user
    if (username) {
      const usernameCheck = await pool.query(
        'SELECT id FROM users WHERE username = $1 AND id != $2',
        [username, id]
      );
      
      if (usernameCheck.rows.length > 0) {
        return res.status(400).json({ error: 'Username already taken' });
      }
    }
    
    // Build update query
    let updateFields = [];
    let values = [];
    let paramCount = 1;
    
    if (username) {
      updateFields.push(`username = $${paramCount}`);
      values.push(username);
      paramCount++;
    }
    
    if (email !== undefined) {
      updateFields.push(`email = $${paramCount}`);
      values.push(email || null);
      paramCount++;
    }
    
    if (password) {
      const passwordHash = await bcrypt.hash(password, 10);
      updateFields.push(`password_hash = $${paramCount}`);
      values.push(passwordHash);
      paramCount++;
    }
    
    if (role !== undefined) {
      updateFields.push(`role = $${paramCount}`);
      values.push(role);
      paramCount++;
    }
    
    if (active !== undefined) {
      updateFields.push(`active = $${paramCount}`);
      values.push(active);
      paramCount++;
    }
    
    updateFields.push(`updated_at = CURRENT_TIMESTAMP`);
    
    values.push(id);
    
    const query = `
      UPDATE users 
      SET ${updateFields.join(', ')}
      WHERE id = $${paramCount}
      RETURNING id, username, email, role, active, created_at
    `;
    
    const result = await pool.query(query, values);
    
    console.log(`User updated: ${result.rows[0].username}`);
    res.json(result.rows[0]);
  } catch (error) {
    console.error('Error updating user:', error);
    res.status(500).json({ error: 'Failed to update user' });
  }
});

// Delete user
app.delete('/api/users/:id', async (req, res) => {
  try {
    const { id } = req.params;
    
    // Don't allow deleting the last admin user
    const adminCount = await pool.query(
      'SELECT COUNT(*) FROM users WHERE role = $1 AND id != $2',
      ['admin', id]
    );
    
    const userToDelete = await pool.query(
      'SELECT role, username FROM users WHERE id = $1',
      [id]
    );
    
    if (userToDelete.rows.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }
    
    if (userToDelete.rows[0].role === 'admin' && parseInt(adminCount.rows[0].count) === 0) {
      return res.status(400).json({ error: 'Cannot delete the last admin user' });
    }
    
    // Delete the user
    await pool.query('DELETE FROM users WHERE id = $1', [id]);
    
    console.log(`User deleted: ${userToDelete.rows[0].username}`);
    res.json({ success: true, message: 'User deleted successfully' });
  } catch (error) {
    console.error('Error deleting user:', error);
    res.status(500).json({ error: 'Failed to delete user' });
  }
});

// Verify user credentials (for login - optional, for future use)
app.post('/api/users/verify', async (req, res) => {
  try {
    const { username, password } = req.body;
    
    if (!username || !password) {
      return res.status(400).json({ error: 'Username and password are required' });
    }
    
    const result = await pool.query(
      'SELECT id, username, email, password_hash, role, active FROM users WHERE username = $1',
      [username]
    );
    
    if (result.rows.length === 0) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }
    
    const user = result.rows[0];
    
    if (!user.active) {
      return res.status(401).json({ error: 'Account is inactive' });
    }
    
    const validPassword = await bcrypt.compare(password, user.password_hash);
    
    if (!validPassword) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }
    
    // Return user info without password
    res.json({
      id: user.id,
      username: user.username,
      email: user.email,
      role: user.role,
      active: user.active
    });
  } catch (error) {
    console.error('Error verifying user:', error);
    res.status(500).json({ error: 'Failed to verify credentials' });
  }
});

// Start server
initDatabase().then(() => {
  app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
  });
});
