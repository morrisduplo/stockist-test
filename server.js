const express = require('express');
const { Pool } = require('pg');
const multer = require('multer');
const xlsx = require('xlsx');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');
const bcrypt = require('bcryptjs');

const app = express();
const PORT = process.env.PORT || 3000;

// PostgreSQL connection
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

// Test database connection
pool.connect((err, client, release) => {
    if (err) {
        console.error('Error connecting to database:', err.stack);
    } else {
        console.log('Connected to PostgreSQL database');
        release();
    }
});

// Middleware
app.use(express.static('public'));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Configure multer for file uploads
const upload = multer({ dest: 'uploads/' });

// Customer name mappings configuration
const customerNameMappings = {
    'ANTENNE - DIRECT UK': 'Antenne Online UK',
    'ANTENNE DIRECT': 'Antenne Online',
    'GARDNERS THE BOOK WHOLESALER': 'Gardners',
    'ANTENNE BOOKS - DIRECT': 'Antenne Direct',
    'KOENIG BOOKS LTD': 'Koenig Books',
    'ANTENNE - EXPORT': 'Antenne Export',
    'FISHPOND WORLD LTD': 'Fishpond',
    'NEWS AND COFFEE LTD': 'News and Coffee',
    'BOOKS ETC. LTD (INTERNET SITE)': 'Books Etc Online',
    'WHITE CUBE LIMITED': 'White Cube',
    'ISSUES MAGAZINE SHOP': 'Issues Shop',
    'ATHENAEUM BOEKHANDEL BV': 'Athenaeum',
    'UNITOM UNIVERSAL TOMORROW LTD': 'Unitom',
    'THE AFFAIRS CIRCULATION LTD': 'The Affairs',
    'PBSHOP.CO.UK LIMITED': 'PB Shop',
    'COEN SLIGTING BOOKIMPORT BV': 'Coen Sligting'
};

// Replace the initBooksonixTable function in your server.js (around line 54-85) with this version:

// Initialize Booksonix table - UPDATED TO USE SKU INSTEAD OF ISBN
async function initBooksonixTable() {
    try {
        // First, check if the table exists and what columns it has
        const tableCheck = await pool.query(`
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = 'booksonix_records'
            ORDER BY ordinal_position;
        `);
        
        if (tableCheck.rows.length === 0) {
            // Table doesn't exist, create it with SKU-based structure
            console.log('Creating new Booksonix table with SKU-based structure...');
            await pool.query(`
                CREATE TABLE booksonix_records (
                    id SERIAL PRIMARY KEY,
                    sku VARCHAR(100) UNIQUE NOT NULL,
                    isbn VARCHAR(50),
                    title VARCHAR(500),
                    author VARCHAR(500),
                    publisher VARCHAR(500),
                    price DECIMAL(10,2),
                    quantity INTEGER DEFAULT 0,
                    format VARCHAR(100),
                    publication_date DATE,
                    description TEXT,
                    category VARCHAR(200),
                    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            `);
            
            // Create indexes
            await pool.query(`CREATE INDEX IF NOT EXISTS idx_booksonix_sku ON booksonix_records(sku)`);
            await pool.query(`CREATE INDEX IF NOT EXISTS idx_booksonix_isbn ON booksonix_records(isbn)`);
            
            console.log('Booksonix table created successfully with SKU-based structure');
        } else {
            // Table exists, check if it needs migration from ISBN to SKU
            const hasSkuColumn = tableCheck.rows.some(row => row.column_name === 'sku');
            const isbnColumn = tableCheck.rows.find(row => row.column_name === 'isbn');
            
            if (!hasSkuColumn) {
                console.log('Migrating Booksonix table from ISBN-based to SKU-based structure...');
                
                // Add SKU column
                await pool.query(`ALTER TABLE booksonix_records ADD COLUMN IF NOT EXISTS sku VARCHAR(100)`);
                
                // Check if there's existing data
                const countResult = await pool.query(`SELECT COUNT(*) as count FROM booksonix_records`);
                const hasData = parseInt(countResult.rows[0].count) > 0;
                
                if (hasData) {
                    // If there's existing data, copy ISBN to SKU for migration
                    console.log('Migrating existing records: copying ISBN to SKU...');
                    await pool.query(`UPDATE booksonix_records SET sku = isbn WHERE sku IS NULL AND isbn IS NOT NULL`);
                    
                    // For any remaining null SKUs, generate a temporary SKU
                    await pool.query(`
                        UPDATE booksonix_records 
                        SET sku = 'TEMP_' || id::text 
                        WHERE sku IS NULL
                    `);
                }
                
                // Drop the old ISBN unique constraint if it exists
                try {
                    await pool.query(`ALTER TABLE booksonix_records DROP CONSTRAINT IF EXISTS booksonix_records_isbn_key`);
                } catch (e) {
                    console.log('No ISBN constraint to drop');
                }
                
                // Make ISBN nullable if it wasn't already
                if (isbnColumn && isbnColumn.is_nullable === 'NO') {
                    await pool.query(`ALTER TABLE booksonix_records ALTER COLUMN isbn DROP NOT NULL`);
                }
                
                // Add unique constraint to SKU
                try {
                    await pool.query(`ALTER TABLE booksonix_records ADD CONSTRAINT booksonix_records_sku_key UNIQUE (sku)`);
                } catch (e) {
                    console.log('SKU unique constraint may already exist');
                }
                
                // Make SKU NOT NULL
                await pool.query(`ALTER TABLE booksonix_records ALTER COLUMN sku SET NOT NULL`);
                
                console.log('Migration complete: Booksonix table now uses SKU as primary identifier');
            } else {
                console.log('Booksonix table already has SKU column, checking constraints...');
                
                // Ensure SKU has proper constraints
                try {
                    // Check if SKU is NOT NULL
                    const skuColumn = tableCheck.rows.find(row => row.column_name === 'sku');
                    if (skuColumn && skuColumn.is_nullable === 'YES') {
                        // First fill any NULL values
                        await pool.query(`
                            UPDATE booksonix_records 
                            SET sku = COALESCE(isbn, 'TEMP_' || id::text) 
                            WHERE sku IS NULL
                        `);
                        await pool.query(`ALTER TABLE booksonix_records ALTER COLUMN sku SET NOT NULL`);
                    }
                    
                    // Ensure unique constraint exists
                    await pool.query(`ALTER TABLE booksonix_records ADD CONSTRAINT booksonix_records_sku_key UNIQUE (sku)`);
                } catch (e) {
                    // Constraint likely already exists
                }
                
                // Ensure ISBN is nullable
                if (isbnColumn && isbnColumn.is_nullable === 'NO') {
                    await pool.query(`ALTER TABLE booksonix_records ALTER COLUMN isbn DROP NOT NULL`);
                }
            }
            
            // Create indexes if they don't exist
            await pool.query(`CREATE INDEX IF NOT EXISTS idx_booksonix_sku ON booksonix_records(sku)`);
            await pool.query(`CREATE INDEX IF NOT EXISTS idx_booksonix_isbn ON booksonix_records(isbn)`);
        }
        
        // Verify final structure
        const finalCheck = await pool.query(`
            SELECT COUNT(*) as total FROM booksonix_records
        `);
        
        console.log(`Booksonix table ready with ${finalCheck.rows[0].total} existing records`);
        
    } catch (err) {
        console.error('Error initializing Booksonix table:', err);
        console.error('Error details:', err.message);
        
        // If there's a critical error, try to at least ensure the table exists
        try {
            await pool.query(`
                CREATE TABLE IF NOT EXISTS booksonix_records (
                    id SERIAL PRIMARY KEY,
                    sku VARCHAR(100),
                    isbn VARCHAR(50),
                    title VARCHAR(500),
                    author VARCHAR(500),
                    publisher VARCHAR(500),
                    price DECIMAL(10,2),
                    quantity INTEGER DEFAULT 0,
                    format VARCHAR(100),
                    publication_date DATE,
                    description TEXT,
                    category VARCHAR(200),
                    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            `);
            console.log('Created basic Booksonix table structure');
        } catch (fallbackErr) {
            console.error('Failed to create fallback table:', fallbackErr);
        }
    }
}

// Also add this temporary endpoint to manually check/fix the Booksonix table
// Add this after your other routes (around line 1000+):

// Temporary endpoint to check Booksonix table status
app.get('/api/booksonix/check-table', async (req, res) => {
    try {
        // Check table structure
        const columns = await pool.query(`
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = 'booksonix_records'
            ORDER BY ordinal_position;
        `);
        
        // Check constraints
        const constraints = await pool.query(`
            SELECT constraint_name, constraint_type
            FROM information_schema.table_constraints
            WHERE table_name = 'booksonix_records';
        `);
        
        // Check record count
        const count = await pool.query(`SELECT COUNT(*) as total FROM booksonix_records`);
        
        // Check sample records
        const samples = await pool.query(`SELECT * FROM booksonix_records LIMIT 5`);
        
        res.json({
            columns: columns.rows,
            constraints: constraints.rows,
            totalRecords: count.rows[0].total,
            sampleRecords: samples.rows,
            status: 'Table check complete'
        });
        
    } catch (err) {
        res.status(500).json({
            error: 'Failed to check table',
            message: err.message,
            detail: err.detail
        });
    }
});

// Temporary endpoint to manually reset the Booksonix table (USE WITH CAUTION!)
app.post('/api/booksonix/reset-table', async (req, res) => {
    try {
        // Only allow this in development or with a special key
        const resetKey = req.body.resetKey;
        if (resetKey !== 'RESET_BOOKSONIX_2024') {
            return res.status(403).json({ error: 'Invalid reset key' });
        }
        
        // Drop and recreate the table
        await pool.query(`DROP TABLE IF EXISTS booksonix_records`);
        
        await pool.query(`
            CREATE TABLE booksonix_records (
                id SERIAL PRIMARY KEY,
                sku VARCHAR(100) UNIQUE NOT NULL,
                isbn VARCHAR(50),
                title VARCHAR(500),
                author VARCHAR(500),
                publisher VARCHAR(500),
                price DECIMAL(10,2),
                quantity INTEGER DEFAULT 0,
                format VARCHAR(100),
                publication_date DATE,
                description TEXT,
                category VARCHAR(200),
                upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);
        
        // Create indexes
        await pool.query(`CREATE INDEX idx_booksonix_sku ON booksonix_records(sku)`);
        await pool.query(`CREATE INDEX idx_booksonix_isbn ON booksonix_records(isbn)`);
        
        res.json({
            success: true,
            message: 'Booksonix table has been reset with SKU-based structure'
        });
        
    } catch (err) {
        res.status(500).json({
            error: 'Failed to reset table',
            message: err.message
        });
    }
});

// Initialize database tables
async function initDatabase() {
    try {
        console.log('Starting database initialization...');
        
        // Create records table
        await pool.query(`
            CREATE TABLE IF NOT EXISTS records (
                id SERIAL PRIMARY KEY,
                order_date DATE,
                customer_name VARCHAR(500),
                title VARCHAR(500),
                book_ean VARCHAR(50),
                quantity INTEGER DEFAULT 0,
                total DECIMAL(10,2) DEFAULT 0,
                country VARCHAR(100) DEFAULT 'Unknown',
                city VARCHAR(100) DEFAULT 'Unknown',
                order_reference VARCHAR(200),
                line_identifier VARCHAR(200),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(order_reference, line_identifier)
            )
        `);

        // Create upload_log table
        await pool.query(`
            CREATE TABLE IF NOT EXISTS upload_log (
                id SERIAL PRIMARY KEY,
                filename VARCHAR(500),
                records_count INTEGER DEFAULT 0,
                upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // Create customer_exclusions table
        await pool.query(`
            CREATE TABLE IF NOT EXISTS customer_exclusions (
                id SERIAL PRIMARY KEY,
                customer_name VARCHAR(500) UNIQUE,
                excluded BOOLEAN DEFAULT true,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // Create users table with correct structure
        await pool.query(`
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(100) UNIQUE NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                password VARCHAR(255) NOT NULL,
                role VARCHAR(50) DEFAULT 'editor',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP
            )
        `);

        // Create indexes for better performance
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_records_customer ON records(customer_name)`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_records_order_ref ON records(order_reference)`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_records_date ON records(order_date)`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_customer_exclusions ON customer_exclusions(customer_name)`);

        // Initialize Booksonix table
        await initBooksonixTable();

        console.log('Database tables created successfully');
        
        // Check if admin user exists
        const adminCheck = await pool.query("SELECT * FROM users WHERE username = 'admin'");
        
        if (adminCheck.rows.length === 0) {
            console.log('Creating admin user...');
            
            // Create admin user with password 'admin123'
            const hashedPassword = await bcrypt.hash('admin123', 10);
            
            await pool.query(
                'INSERT INTO users (username, email, password, role) VALUES ($1, $2, $3, $4)',
                ['admin', 'admin@antennebooks.com', hashedPassword, 'admin']
            );
            
            console.log('=================================');
            console.log('Admin user created successfully!');
            console.log('Username: admin');
            console.log('Password: admin123');
            console.log('IMPORTANT: Please change this password immediately after first login!');
            console.log('=================================');
        } else {
            console.log('Admin user already exists');
        }
        
    } catch (err) {
        console.error('Error initializing database:', err);
        throw err;
    }
}

// Initialize database on startup
initDatabase().catch(err => {
    console.error('Failed to initialize database:', err);
});

// Helper function to apply customer name mapping
function applyMapping(customerName) {
    return customerNameMappings[customerName] || customerName;
}

// Helper function to log uploads
async function logUpload(filename, recordCount) {
    try {
        await pool.query(
            'INSERT INTO upload_log (filename, records_count) VALUES ($1, $2)',
            [filename, recordCount]
        );
    } catch (err) {
        console.error('Error logging upload:', err);
    }
}

// =============================================
// PAGE ROUTES
// =============================================

// Home page
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Upload page
app.get('/upload', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'upload.html'));
});

// Customers page
app.get('/customers', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'customers.html'));
});

// Reports page
app.get('/reports', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'reports.html'));
});

// Settings page
app.get('/settings', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'settings.html'));
});

// Login page (explicit route)
app.get('/login', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'login.html'));
});

// Login page with .html extension
app.get('/login.html', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'login.html'));
});

// Data Upload main page
app.get('/data-upload', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'data-upload.html'));
});

// Data Upload sub-pages
app.get('/data-upload/page1', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'data-upload-page1.html'));
});

app.get('/data-upload/page2', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'data-upload-page2.html'));
});

app.get('/data-upload/page3', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'data-upload-page3.html'));
});

app.get('/data-upload/page4', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'data-upload-page4.html'));
});

app.get('/data-upload/page5', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'data-upload-page5.html'));
});

app.get('/data-upload/page6', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'data-upload-page6.html'));
});

// =============================================
// BOOKSONIX ROUTES - UPDATED TO USE SKU
// =============================================

// Page route for Booksonix
app.get('/booksonix', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'booksonix.html'));
});

// Upload Booksonix data - UPDATED TO USE SKU AS PRIMARY IDENTIFIER
app.post('/api/booksonix/upload', upload.single('booksonixFile'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'No file uploaded' });
    }

    console.log('Processing Booksonix file:', req.file.originalname);

    try {
        const workbook = xlsx.readFile(req.file.path);
        const sheetName = workbook.SheetNames[0];
        const data = xlsx.utils.sheet_to_json(workbook.Sheets[sheetName]);

        console.log('Total rows in Excel file:', data.length);
        if (data.length > 0) {
            console.log('Sample row columns:', Object.keys(data[0]));
        }

        let newRecords = 0;
        let duplicates = 0;
        let errors = 0;
        let skippedNoSku = 0;

        for (const row of data) {
            // Look for SKU in multiple possible column names
            const sku = row['SKU'] || row['sku'] || row['Sku'] || 
                       row['Product SKU'] || row['Product Code'] || 
                       row['Item Code'] || row['Code'] || '';
            
            if (!sku) {
                console.log('Skipping row - no SKU found. Row data:', Object.keys(row));
                skippedNoSku++;
                errors++;
                continue; // Skip records without SKU
            }

            // ISBN is now optional
            const isbn = row['ISBN'] || row['ISBN13'] || row['EAN'] || row['isbn'] || '';

            try {
                // Try to insert, but update if SKU already exists
                const result = await pool.query(
                    `INSERT INTO booksonix_records 
                    (sku, isbn, title, author, publisher, price, quantity, format, publication_date, description, category) 
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    ON CONFLICT (sku) 
                    DO UPDATE SET 
                        isbn = COALESCE(NULLIF(EXCLUDED.isbn, ''), booksonix_records.isbn),
                        title = EXCLUDED.title,
                        author = EXCLUDED.author,
                        publisher = EXCLUDED.publisher,
                        price = EXCLUDED.price,
                        quantity = booksonix_records.quantity + EXCLUDED.quantity,
                        format = EXCLUDED.format,
                        publication_date = EXCLUDED.publication_date,
                        description = EXCLUDED.description,
                        category = EXCLUDED.category,
                        last_updated = CURRENT_TIMESTAMP
                    RETURNING id, (xmax = 0) AS inserted`,
                    [
                        sku,
                        isbn || null, // Store null if no ISBN
                        row['Title'] || row['Product Title'] || row['title'] || row['Product'] || '',
                        row['Author'] || row['Authors'] || row['author'] || '',
                        row['Publisher'] || row['publisher'] || '',
                        parseFloat(row['Price'] || row['RRP'] || row['price'] || 0) || 0,
                        parseInt(row['Quantity'] || row['Stock'] || row['Qty'] || row['quantity'] || 0) || 0,
                        row['Format'] || row['Binding'] || row['format'] || '',
                        row['Publication Date'] || row['Pub Date'] || null,
                        row['Description'] || row['description'] || '',
                        row['Category'] || row['Subject'] || row['category'] || ''
                    ]
                );

                if (result.rows[0].inserted) {
                    newRecords++;
                    console.log('Inserted new record with SKU:', sku);
                } else {
                    duplicates++;
                    console.log('Updated existing record with SKU:', sku);
                }
            } catch (err) {
                console.error('Error inserting Booksonix record with SKU', sku, ':', err.message);
                errors++;
            }
        }

        // Clean up uploaded file
        fs.unlinkSync(req.file.path);

        console.log('Upload summary:', {
            totalRows: data.length,
            newRecords,
            duplicates,
            errors,
            skippedNoSku
        });

        res.json({ 
            success: true, 
            message: `Processed ${data.length} records. New: ${newRecords}, Updated: ${duplicates}, Errors: ${errors}`,
            newRecords: newRecords,
            duplicates: duplicates,
            errors: errors,
            skippedNoSku: skippedNoSku
        });

    } catch (error) {
        console.error('Booksonix upload error:', error);
        if (fs.existsSync(req.file.path)) {
            fs.unlinkSync(req.file.path);
        }
        res.status(500).json({ error: error.message });
    }
});

// Get Booksonix records
app.get('/api/booksonix/records', async (req, res) => {
    try {
        const result = await pool.query(
            'SELECT * FROM booksonix_records ORDER BY upload_date DESC LIMIT 500'
        );
        
        res.json({ records: result.rows });
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

// Get Booksonix statistics - UPDATED TO COUNT UNIQUE SKUs
app.get('/api/booksonix/stats', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT sku) as unique_skus,
                SUM(quantity) as total_quantity,
                COUNT(DISTINCT publisher) as publishers
            FROM booksonix_records
        `);
        
        res.json({
            totalRecords: result.rows[0].total_records || 0,
            uniqueSKUs: result.rows[0].unique_skus || 0,  // Changed from uniqueISBNs
            totalQuantity: result.rows[0].total_quantity || 0,
            totalPublishers: result.rows[0].publishers || 0
        });
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

// =============================================
// API ROUTES
// =============================================

// Authentication endpoint
app.post('/api/login', async (req, res) => {
    const { username, password } = req.body;

    console.log('Login attempt for username:', username);

    if (!username || !password) {
        return res.status(400).json({ error: 'Username and password required' });
    }

    try {
        const result = await pool.query(
            'SELECT * FROM users WHERE username = $1 OR email = $1',
            [username]
        );

        console.log('Found users:', result.rows.length);

        if (result.rows.length === 0) {
            console.log('No user found with username:', username);
            return res.status(401).json({ error: 'Invalid credentials' });
        }

        const user = result.rows[0];
        console.log('User found:', user.username, 'Role:', user.role);
        
        const validPassword = await bcrypt.compare(password, user.password);
        console.log('Password valid:', validPassword);
        
        if (!validPassword) {
            console.log('Invalid password for user:', username);
            return res.status(401).json({ error: 'Invalid credentials' });
        }

        // Update last login
        await pool.query(
            'UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = $1',
            [user.id]
        );

        console.log('Login successful for:', username);

        res.json({
            success: true,
            user: {
                id: user.id,
                username: user.username,
                email: user.email,
                role: user.role
            }
        });
    } catch (err) {
        console.error('Login database error:', err);
        res.status(500).json({ error: 'Database error during login' });
    }
});

// Upload endpoint
app.post('/upload', upload.single('excelFile'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'No file uploaded' });
    }

    const dataType = req.body.dataType || 'gazelle';
    console.log('Processing file:', req.file.originalname, 'Type:', dataType);

    try {
        let recordsProcessed = 0;
        let duplicatesSkipped = 0;

        if (dataType === 'shopify' || req.file.originalname.toLowerCase().endsWith('.csv')) {
            // Process CSV file (Shopify format)
            const results = [];
            const stream = fs.createReadStream(req.file.path)
                .pipe(csv())
                .on('data', (data) => results.push(data))
                .on('end', async () => {
                    for (const row of results) {
                        const orderReference = row['Name'] || row['Order Name'] || '';
                        const lineItem = row['Lineitem name'] || '';
                        
                        if (orderReference) {
                            const lineIdentifier = `${orderReference}_${lineItem}`;
                            
                            try {
                                const insertResult = await pool.query(
                                    `INSERT INTO records 
                                    (order_date, customer_name, title, book_ean, quantity, total, country, city, order_reference, line_identifier) 
                                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                                    ON CONFLICT (order_reference, line_identifier) DO NOTHING
                                    RETURNING id`,
                                    [
                                        row['Created at'] || null,
                                        applyMapping(row['Shipping Name'] || 'Unknown'),
                                        row['Lineitem name'] || '',
                                        row['Lineitem sku'] || '',
                                        parseInt(row['Lineitem quantity']) || 0,
                                        parseFloat(row['Lineitem price']) || 0,
                                        'Unknown',
                                        'Unknown',
                                        orderReference,
                                        lineIdentifier
                                    ]
                                );
                                
                                if (insertResult.rows.length > 0) {
                                    recordsProcessed++;
                                } else {
                                    duplicatesSkipped++;
                                }
                            } catch (err) {
                                console.error('Error inserting CSV row:', err);
                            }
                        }
                    }
                    
                    await logUpload(req.file.originalname, recordsProcessed);
                    fs.unlinkSync(req.file.path);
                    
                    res.json({ 
                        success: true, 
                        message: `Uploaded ${recordsProcessed} records, ${duplicatesSkipped} duplicates skipped`,
                        inserted: recordsProcessed,
                        skipped: duplicatesSkipped
                    });
                });
        } else {
            // Process Excel file (Gazelle format)
            const workbook = xlsx.readFile(req.file.path);
            const sheetName = workbook.SheetNames[0];
            const data = xlsx.utils.sheet_to_json(workbook.Sheets[sheetName]);

            for (const row of data) {
                const orderReference = row['Invoice'] || row['Order Reference'] || '';
                const productTitle = row['Title'] || '';
                
                if (orderReference) {
                    const lineIdentifier = `${orderReference}_${productTitle}`;
                    
                    try {
                        const insertResult = await pool.query(
                            `INSERT INTO records 
                            (order_date, customer_name, title, book_ean, quantity, total, country, city, order_reference, line_identifier) 
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                            ON CONFLICT (order_reference, line_identifier) DO NOTHING
                            RETURNING id`,
                            [
                                row['Date'] || null,
                                applyMapping(row['Customer'] || 'Unknown'),
                                productTitle,
                                row['ISBN'] || row['EAN'] || '',
                                parseInt(row['Quantity']) || parseInt(row['Qty']) || 0,
                                parseFloat(row['Total']) || parseFloat(row['Amount']) || 0,
                                'Unknown',
                                'Unknown',
                                orderReference,
                                lineIdentifier
                            ]
                        );
                        
                        if (insertResult.rows.length > 0) {
                            recordsProcessed++;
                        } else {
                            duplicatesSkipped++;
                        }
                    } catch (err) {
                        console.error('Error inserting Excel row:', err);
                    }
                }
            }

            await logUpload(req.file.originalname, recordsProcessed);
            fs.unlinkSync(req.file.path);
            
            res.json({ 
                success: true, 
                message: `Uploaded ${recordsProcessed} records, ${duplicatesSkipped} duplicates skipped`,
                inserted: recordsProcessed,
                skipped: duplicatesSkipped
            });
        }
    } catch (error) {
        console.error('Upload error:', error);
        if (fs.existsSync(req.file.path)) {
            fs.unlinkSync(req.file.path);
        }
        res.status(500).json({ error: error.message });
    }
});

// Get records with pagination
app.get('/records', async (req, res) => {
    const page = parseInt(req.query.page) || 1;
    const limit = 500;
    const offset = (page - 1) * limit;

    try {
        // Get total count
        const countResult = await pool.query('SELECT COUNT(*) FROM records');
        const totalRecords = parseInt(countResult.rows[0].count);
        const totalPages = Math.ceil(totalRecords / limit);

        // Get records for current page
        const result = await pool.query(
            'SELECT * FROM records ORDER BY id DESC LIMIT $1 OFFSET $2',
            [limit, offset]
        );

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
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

// Get upload log
app.get('/upload-log', async (req, res) => {
    try {
        const result = await pool.query('SELECT * FROM upload_log ORDER BY upload_date DESC LIMIT 20');
        res.json(result.rows);
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

// Get customers
app.get('/api/customers', async (req, res) => {
    try {
        // First, get aggregated data per customer
        const result = await pool.query(`
            WITH customer_aggregates AS (
                SELECT 
                    customer_name,
                    COUNT(*) as total_orders,
                    SUM(quantity) as total_quantity,
                    SUM(total) as total_revenue,
                    MAX(order_date) as last_order
                FROM records
                WHERE customer_name IS NOT NULL
                GROUP BY customer_name
            ),
            customer_locations AS (
                SELECT DISTINCT ON (customer_name)
                    customer_name,
                    country,
                    city
                FROM records
                WHERE customer_name IS NOT NULL
                ORDER BY customer_name, id DESC
            )
            SELECT 
                ca.customer_name,
                COALESCE(cl.country, 'Unknown') as country,
                COALESCE(cl.city, 'Unknown') as city,
                ca.total_orders,
                ca.total_quantity,
                ca.total_revenue,
                ca.last_order,
                CASE WHEN ce.excluded = true THEN true ELSE false END as excluded
            FROM customer_aggregates ca
            LEFT JOIN customer_locations cl ON ca.customer_name = cl.customer_name
            LEFT JOIN customer_exclusions ce ON ca.customer_name = ce.customer_name
            ORDER BY ca.customer_name
        `);

        const stats = await pool.query(`
            SELECT 
                COUNT(DISTINCT customer_name) as total_customers,
                COUNT(DISTINCT country) as total_countries,
                COUNT(*) as total_orders
            FROM records
            WHERE customer_name IS NOT NULL
        `);

        res.json({
            customers: result.rows,
            stats: stats.rows[0]
        });
    } catch (err) {
        console.error('Database error in /api/customers:', err);
        res.status(500).json({ 
            error: 'Database error', 
            details: err.message 
        });
    }
});

// Update customer location
app.post('/api/customers/update', async (req, res) => {
    const { customerName, field, value } = req.body;

    try {
        if (field === 'country' || field === 'city') {
            await pool.query(
                `UPDATE records SET ${field} = $1 WHERE customer_name = $2`,
                [value, customerName]
            );
            res.json({ success: true });
        } else {
            res.status(400).json({ error: 'Invalid field' });
        }
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

// Bulk update customer locations
app.post('/api/customers/bulk-update', async (req, res) => {
    const { updates } = req.body;
    let updatedCount = 0;

    try {
        for (const update of updates) {
            const result = await pool.query(
                'UPDATE records SET country = $1, city = $2 WHERE customer_name = $3',
                [update.country, update.city, update.customerName]
            );
            updatedCount += result.rowCount;
        }
        res.json({ success: true, updated: updatedCount });
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

// Exclude/Include customers
app.post('/api/customers/exclude', async (req, res) => {
    const { customers, excluded } = req.body;

    try {
        for (const customerName of customers) {
            await pool.query(
                `INSERT INTO customer_exclusions (customer_name, excluded) 
                 VALUES ($1, $2) 
                 ON CONFLICT (customer_name) 
                 DO UPDATE SET excluded = $2, updated_at = CURRENT_TIMESTAMP`,
                [customerName, excluded]
            );
        }
        res.json({ success: true });
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

// Get titles for autocomplete
app.get('/api/titles', async (req, res) => {
    try {
        const result = await pool.query(
            'SELECT DISTINCT title FROM records WHERE title IS NOT NULL ORDER BY title'
        );
        res.json(result.rows);
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

// Generate report
app.post('/api/generate-report', async (req, res) => {
    try {
        const { publisher, startDate, endDate, titles } = req.body;
        
        console.log('Generate report request:', { publisher, startDate, endDate, titlesCount: titles.length });
        
        if (!publisher || !startDate || !endDate || !titles || titles.length === 0) {
            return res.status(400).json({ error: 'Missing required parameters' });
        }
        
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

// Update record
app.post('/api/update-record', async (req, res) => {
    const { id, customer_name, country, city, title } = req.body;

    try {
        await pool.query(
            'UPDATE records SET customer_name = $1, country = $2, city = $3, title = $4 WHERE id = $5',
            [customer_name, country, city, title, id]
        );
        res.json({ success: true });
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

// Settings endpoints
app.get('/api/mappings', async (req, res) => {
    try {
        const mappings = Object.entries(customerNameMappings).map(([original, display], index) => ({
            id: index + 1,
            original_name: original,
            display_name: display
        }));
        res.json(mappings);
    } catch (err) {
        console.error('Error loading mappings:', err);
        res.status(500).json({ error: 'Error loading mappings' });
    }
});

app.post('/api/mappings', async (req, res) => {
    const { original_name, display_name } = req.body;
    customerNameMappings[original_name] = display_name;
    res.json({ success: true });
});

app.delete('/api/mappings/:id', async (req, res) => {
    // Note: This is a simplified version - in production you'd want to persist these
    res.json({ success: true });
});

app.get('/api/stats', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT customer_name) as total_customers,
                COUNT(DISTINCT title) as total_titles
            FROM records
        `);
        res.json(result.rows[0]);
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

// User management endpoints
app.get('/api/users', async (req, res) => {
    try {
        const result = await pool.query(
            'SELECT id, username, email, role, created_at, last_login FROM users ORDER BY id'
        );
        res.json(result.rows);
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

app.post('/api/users', async (req, res) => {
    const { username, email, password, role } = req.body;

    if (!username || !email || !password) {
        return res.status(400).json({ error: 'Username, email, and password are required' });
    }

    try {
        const hashedPassword = await bcrypt.hash(password, 10);
        const result = await pool.query(
            'INSERT INTO users (username, email, password, role) VALUES ($1, $2, $3, $4) RETURNING id',
            [username, email, hashedPassword, role || 'editor']
        );
        res.json({ success: true, id: result.rows[0].id });
    } catch (err) {
        if (err.code === '23505') {
            res.status(400).json({ error: 'Username or email already exists' });
        } else {
            console.error('Database error:', err);
            res.status(500).json({ error: 'Database error' });
        }
    }
});

app.put('/api/users/:id', async (req, res) => {
    const { id } = req.params;
    const { username, email, password, role } = req.body;

    try {
        let query = 'UPDATE users SET username = $1, email = $2, role = $3';
        let params = [username, email, role];
        let paramIndex = 4;

        if (password) {
            const hashedPassword = await bcrypt.hash(password, 10);
            query += `, password = $${paramIndex}`;
            params.push(hashedPassword);
            paramIndex++;
        }

        query += ` WHERE id = $${paramIndex}`;
        params.push(id);

        const result = await pool.query(query, params);

        if (result.rowCount === 0) {
            return res.status(404).json({ error: 'User not found' });
        }

        res.json({ success: true });
    } catch (err) {
        if (err.code === '23505') {
            res.status(400).json({ error: 'Username or email already exists' });
        } else {
            console.error('Database error:', err);
            res.status(500).json({ error: 'Database error' });
        }
    }
});

app.delete('/api/users/:id', async (req, res) => {
    const { id } = req.params;

    try {
        const userResult = await pool.query('SELECT role FROM users WHERE id = $1', [id]);
        if (userResult.rows.length === 0) {
            return res.status(404).json({ error: 'User not found' });
        }

        if (userResult.rows[0].role === 'admin') {
            const adminCount = await pool.query(
                "SELECT COUNT(*) as count FROM users WHERE role = 'admin' AND id != $1",
                [id]
            );
            
            if (adminCount.rows[0].count === '0') {
                return res.status(400).json({ error: 'Cannot delete the last admin user' });
            }
        }

        await pool.query('DELETE FROM users WHERE id = $1', [id]);
        res.json({ success: true });
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

app.get('/api/users/:id', async (req, res) => {
    const { id } = req.params;
    
    try {
        const result = await pool.query(
            'SELECT id, username, email, role, created_at, last_login FROM users WHERE id = $1',
            [id]
        );
        
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'User not found' });
        }
        
        res.json(result.rows[0]);
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

// Put settings
app.put('/api/settings', async (req, res) => {
    // In a production app, you'd save these settings to the database
    console.log('Settings update:', req.body);
    res.json({ success: true });
});

// Health check endpoint
app.get('/health', async (req, res) => {
    try {
        await pool.query('SELECT 1');
        res.json({ 
            status: 'healthy', 
            timestamp: new Date().toISOString(),
            database: 'connected'
        });
    } catch (err) {
        res.status(503).json({ 
            status: 'unhealthy', 
            timestamp: new Date().toISOString(),
            database: 'disconnected',
            error: err.message
        });
    }
});

// Start server
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
    console.log(`Visit http://localhost:${PORT} to access the application`);
    console.log('=================================');
    console.log('IMPORTANT: Default admin credentials');
    console.log('Username: admin');
    console.log('Password: admin123');
    console.log('Please change this password after first login!');
    console.log('=================================');
});

// Graceful shutdown
process.on('SIGINT', () => {
    console.log('\nShutting down gracefully...');
    pool.end(() => {
        console.log('Database pool closed.');
        process.exit(0);
    });
});
