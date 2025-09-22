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

// Initialize database tables
async function initDatabase() {
    try {
        // Create tables if they don't exist
        await pool.query(`
            CREATE TABLE IF NOT EXISTS stockist_data (
                id SERIAL PRIMARY KEY,
                customer_name TEXT,
                display_name TEXT,
                location TEXT,
                quantity INTEGER,
                order_date TEXT,
                order_reference TEXT UNIQUE,
                item_name TEXT,
                variant_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        await pool.query(`
            CREATE TABLE IF NOT EXISTS customer_mappings (
                id SERIAL PRIMARY KEY,
                original_name TEXT UNIQUE,
                display_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        await pool.query(`
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username TEXT UNIQUE,
                email TEXT UNIQUE,
                password TEXT,
                role TEXT DEFAULT 'viewer',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP
            )
        `);

        await pool.query(`
            CREATE TABLE IF NOT EXISTS settings (
                id SERIAL PRIMARY KEY,
                key TEXT UNIQUE,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        await pool.query(`
            CREATE TABLE IF NOT EXISTS excluded_customers (
                id SERIAL PRIMARY KEY,
                customer_name TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // Create indexes for better performance
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_stockist_customer ON stockist_data(customer_name)`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_stockist_order_ref ON stockist_data(order_reference)`);

        // Create default admin user if no users exist
        const userCount = await pool.query('SELECT COUNT(*) as count FROM users');
        if (userCount.rows[0].count === '0') {
            const defaultPassword = await bcrypt.hash('admin123', 10);
            await pool.query(
                'INSERT INTO users (username, email, password, role) VALUES ($1, $2, $3, $4)',
                ['admin', 'admin@antennebooks.com', defaultPassword, 'admin']
            );
            console.log('Default admin user created (username: admin, password: admin123)');
        }

        console.log('Database tables initialized successfully');
    } catch (err) {
        console.error('Error initializing database:', err);
    }
}

// Initialize database on startup
initDatabase();

// Routes
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Upload endpoint
app.post('/upload', upload.single('file'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'No file uploaded' });
    }

    const filePath = req.file.path;
    let successCount = 0;
    let errorCount = 0;
    let duplicateCount = 0;

    try {
        if (req.file.originalname.endsWith('.xlsx') || req.file.originalname.endsWith('.xls')) {
            // Handle Excel files (Gazelle format)
            const workbook = xlsx.readFile(filePath);
            const sheetName = workbook.SheetNames[0];
            const sheet = workbook.Sheets[sheetName];
            const data = xlsx.utils.sheet_to_json(sheet);

            for (const row of data) {
                const customer = row['Customer'] || row['customer'] || '';
                const quantity = parseInt(row['Total Quantity'] || row['Quantity'] || row['quantity'] || 0);
                const orderDate = row['Order Date'] || row['Date'] || '';
                const orderRef = row['Order Reference'] || row['Reference'] || row['Order #'] || '';
                const itemName = row['Item Name'] || row['Product'] || '';
                const variantName = row['Variant Name'] || row['Variant'] || '';

                if (customer && orderRef) {
                    try {
                        // Get display name from mappings
                        const mappingResult = await pool.query(
                            'SELECT display_name FROM customer_mappings WHERE original_name = $1',
                            [customer]
                        );
                        const displayName = mappingResult.rows[0]?.display_name || customer;

                        // Insert or ignore if duplicate
                        const insertResult = await pool.query(
                            `INSERT INTO stockist_data 
                            (customer_name, display_name, quantity, order_date, order_reference, item_name, variant_name) 
                            VALUES ($1, $2, $3, $4, $5, $6, $7)
                            ON CONFLICT (order_reference) DO NOTHING
                            RETURNING id`,
                            [customer, displayName, quantity, orderDate, orderRef, itemName, variantName]
                        );

                        if (insertResult.rows.length > 0) {
                            successCount++;
                        } else {
                            duplicateCount++;
                        }
                    } catch (error) {
                        console.error('Error inserting record:', error);
                        errorCount++;
                    }
                }
            }
        } else {
            // Handle CSV files (Shopify format)
            const results = [];
            await new Promise((resolve, reject) => {
                fs.createReadStream(filePath)
                    .pipe(csv())
                    .on('data', (data) => results.push(data))
                    .on('end', () => resolve())
                    .on('error', reject);
            });

            for (const row of results) {
                const customer = row['Shipping Name'] || row['Customer'] || '';
                const quantity = parseInt(row['Lineitem quantity'] || row['Quantity'] || 0);
                const orderDate = row['Created at'] || row['Date'] || '';
                const orderRef = row['Name'] || row['Order'] || '';
                const itemName = row['Lineitem name'] || row['Product'] || '';
                const variantName = row['Lineitem variant'] || '';
                const location = row['Shipping City'] || row['Shipping Town/City'] || '';

                if (customer && orderRef) {
                    try {
                        // Get display name from mappings
                        const mappingResult = await pool.query(
                            'SELECT display_name FROM customer_mappings WHERE original_name = $1',
                            [customer]
                        );
                        const displayName = mappingResult.rows[0]?.display_name || customer;

                        // Insert or ignore if duplicate
                        const insertResult = await pool.query(
                            `INSERT INTO stockist_data 
                            (customer_name, display_name, location, quantity, order_date, order_reference, item_name, variant_name) 
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                            ON CONFLICT (order_reference) DO NOTHING
                            RETURNING id`,
                            [customer, displayName, location, quantity, orderDate, orderRef, itemName, variantName]
                        );

                        if (insertResult.rows.length > 0) {
                            successCount++;
                        } else {
                            duplicateCount++;
                        }
                    } catch (error) {
                        console.error('Error inserting record:', error);
                        errorCount++;
                    }
                }
            }
        }

        // Clean up uploaded file
        fs.unlinkSync(filePath);

        res.json({
            success: true,
            message: `Upload complete. ${successCount} records added, ${duplicateCount} duplicates skipped, ${errorCount} errors.`,
            stats: { success: successCount, duplicates: duplicateCount, errors: errorCount }
        });

    } catch (error) {
        console.error('Upload error:', error);
        if (fs.existsSync(filePath)) {
            fs.unlinkSync(filePath);
        }
        res.status(500).json({ error: 'Failed to process file: ' + error.message });
    }
});

// Get customers endpoint
app.get('/api/customers', async (req, res) => {
    const search = req.query.search || '';
    const excluded = req.query.excluded === 'true';

    try {
        let query = `
            SELECT DISTINCT 
                COALESCE(sd.display_name, sd.customer_name) as display_name,
                sd.customer_name,
                sd.location,
                COUNT(*) as order_count,
                SUM(sd.quantity) as total_quantity,
                CASE WHEN ec.customer_name IS NOT NULL THEN 1 ELSE 0 END as is_excluded
            FROM stockist_data sd
            LEFT JOIN excluded_customers ec ON sd.customer_name = ec.customer_name
        `;

        const params = [];
        let paramIndex = 1;

        if (search) {
            query += ` WHERE (sd.customer_name ILIKE $${paramIndex} OR sd.display_name ILIKE $${paramIndex + 1} OR sd.location ILIKE $${paramIndex + 2})`;
            params.push(`%${search}%`, `%${search}%`, `%${search}%`);
            paramIndex += 3;
        }

        query += ` GROUP BY sd.customer_name, sd.display_name, sd.location, ec.customer_name`;

        if (!excluded) {
            query = `SELECT * FROM (${query}) AS subquery WHERE is_excluded = 0`;
        }

        query += ` ORDER BY display_name`;

        const result = await pool.query(query, params);
        res.json(result.rows);
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error: ' + err.message });
    }
});

// Update customer location
app.put('/api/customers/:name/location', async (req, res) => {
    const { name } = req.params;
    const { location } = req.body;

    try {
        const result = await pool.query(
            'UPDATE stockist_data SET location = $1 WHERE customer_name = $2',
            [location, name]
        );
        res.json({ success: true, changes: result.rowCount });
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

// Exclude/include customer
app.post('/api/customers/:name/exclude', async (req, res) => {
    const { name } = req.params;

    try {
        await pool.query(
            'INSERT INTO excluded_customers (customer_name) VALUES ($1) ON CONFLICT (customer_name) DO NOTHING',
            [name]
        );
        res.json({ success: true });
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

app.delete('/api/customers/:name/exclude', async (req, res) => {
    const { name } = req.params;

    try {
        await pool.query(
            'DELETE FROM excluded_customers WHERE customer_name = $1',
            [name]
        );
        res.json({ success: true });
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

// Get unique titles
app.get('/api/titles', async (req, res) => {
    try {
        const result = await pool.query(
            `SELECT DISTINCT item_name 
             FROM stockist_data 
             WHERE item_name IS NOT NULL AND item_name != '' 
             ORDER BY item_name`
        );
        res.json(result.rows.map(row => row.item_name));
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

// Generate report
app.post('/api/generate-report', async (req, res) => {
    const { titles, startDate, endDate, excludeNoLocation } = req.body;

    try {
        let query = `
            SELECT 
                COALESCE(sd.display_name, sd.customer_name) as customer_name,
                sd.location,
                sd.item_name,
                sd.variant_name,
                SUM(sd.quantity) as total_quantity,
                COUNT(DISTINCT sd.order_reference) as order_count
            FROM stockist_data sd
            LEFT JOIN excluded_customers ec ON sd.customer_name = ec.customer_name
            WHERE ec.customer_name IS NULL
        `;

        const params = [];
        let paramIndex = 1;

        if (titles && titles.length > 0) {
            const placeholders = titles.map((_, i) => `$${paramIndex + i}`).join(',');
            query += ` AND sd.item_name IN (${placeholders})`;
            params.push(...titles);
            paramIndex += titles.length;
        }

        if (startDate) {
            query += ` AND sd.order_date::date >= $${paramIndex}::date`;
            params.push(startDate);
            paramIndex++;
        }

        if (endDate) {
            query += ` AND sd.order_date::date <= $${paramIndex}::date`;
            params.push(endDate);
            paramIndex++;
        }

        if (excludeNoLocation) {
            query += ` AND sd.location IS NOT NULL AND sd.location != ''`;
        }

        query += ` GROUP BY sd.customer_name, sd.display_name, sd.location, sd.item_name, sd.variant_name
                   ORDER BY customer_name, sd.item_name`;

        const result = await pool.query(query, params);
        res.json(result.rows);
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error: ' + err.message });
    }
});

// Customer name mappings endpoints
app.get('/api/mappings', async (req, res) => {
    try {
        const result = await pool.query(
            'SELECT * FROM customer_mappings ORDER BY original_name'
        );
        res.json(result.rows);
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

app.post('/api/mappings', async (req, res) => {
    const { original_name, display_name } = req.body;

    if (!original_name || !display_name) {
        return res.status(400).json({ error: 'Both original_name and display_name are required' });
    }

    try {
        const result = await pool.query(
            `INSERT INTO customer_mappings (original_name, display_name) 
             VALUES ($1, $2) 
             ON CONFLICT (original_name) 
             DO UPDATE SET display_name = $2
             RETURNING id`,
            [original_name, display_name]
        );

        // Update existing records with the new display name
        await pool.query(
            'UPDATE stockist_data SET display_name = $1 WHERE customer_name = $2',
            [display_name, original_name]
        );

        res.json({ success: true, id: result.rows[0].id });
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

app.delete('/api/mappings/:id', async (req, res) => {
    const { id } = req.params;

    try {
        // First get the mapping to know which records to update
        const mappingResult = await pool.query(
            'SELECT original_name FROM customer_mappings WHERE id = $1',
            [id]
        );

        if (mappingResult.rows.length === 0) {
            return res.status(404).json({ error: 'Mapping not found' });
        }

        const originalName = mappingResult.rows[0].original_name;

        // Delete the mapping
        await pool.query('DELETE FROM customer_mappings WHERE id = $1', [id]);

        // Reset display names to original names
        await pool.query(
            'UPDATE stockist_data SET display_name = customer_name WHERE customer_name = $1',
            [originalName]
        );

        res.json({ success: true });
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

// Data management endpoints
app.get('/api/stats', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT customer_name) as total_customers,
                COUNT(DISTINCT item_name) as total_titles
            FROM stockist_data
        `);
        res.json(result.rows[0]);
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

app.get('/api/export-all', async (req, res) => {
    try {
        const result = await pool.query(
            'SELECT * FROM stockist_data ORDER BY order_date DESC'
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'No data to export' });
        }

        // Convert to CSV
        const headers = Object.keys(result.rows[0]).join(',');
        const csvData = result.rows.map(row => 
            Object.values(row).map(val => 
                typeof val === 'string' && val.includes(',') ? `"${val}"` : val
            ).join(',')
        ).join('\n');
        
        res.setHeader('Content-Type', 'text/csv');
        res.setHeader('Content-Disposition', 'attachment; filename=stockist_data.csv');
        res.send(headers + '\n' + csvData);
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

app.post('/api/backup', async (req, res) => {
    // PostgreSQL backup would require pg_dump which is complex in this environment
    // For now, we'll export all data as JSON
    try {
        const tables = ['stockist_data', 'customer_mappings', 'users', 'settings', 'excluded_customers'];
        const backup = {};

        for (const table of tables) {
            const result = await pool.query(`SELECT * FROM ${table}`);
            backup[table] = result.rows;
        }

        const backupFile = `backup_${Date.now()}.json`;
        const backupPath = path.join(__dirname, 'backups', backupFile);
        
        if (!fs.existsSync(path.join(__dirname, 'backups'))) {
            fs.mkdirSync(path.join(__dirname, 'backups'));
        }

        fs.writeFileSync(backupPath, JSON.stringify(backup, null, 2));
        res.json({ success: true, backup: backupFile });
    } catch (err) {
        console.error('Backup error:', err);
        res.status(500).json({ error: 'Backup failed' });
    }
});

app.delete('/api/clear-data', async (req, res) => {
    try {
        const result = await pool.query('DELETE FROM stockist_data');
        res.json({ success: true, deleted: result.rowCount });
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

app.delete('/api/remove-old-records', async (req, res) => {
    try {
        const sixMonthsAgo = new Date();
        sixMonthsAgo.setMonth(sixMonthsAgo.getMonth() - 6);
        
        const result = await pool.query(
            'DELETE FROM stockist_data WHERE order_date::date < $1::date',
            [sixMonthsAgo.toISOString()]
        );
        res.json({ success: true, deleted_count: result.rowCount });
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

app.post('/api/merge-duplicates', async (req, res) => {
    try {
        const result = await pool.query(`
            UPDATE stockist_data 
            SET display_name = customer_name 
            WHERE display_name IS NULL OR display_name = ''
        `);
        res.json({ success: true, merged_count: result.rowCount });
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

app.post('/api/users', async (req, res) => {
    const { username, email, password, role } = req.body;

    if (!username || !email || !password) {
        return res.status(400).json({ error: 'Username, email, and password are required' });
    }

    try {
        const hashedPassword = await bcrypt.hash(password, 10);
        const result = await pool.query(
            'INSERT INTO users (username, email, password, role) VALUES ($1, $2, $3, $4) RETURNING id',
            [username, email, hashedPassword, role || 'viewer']
        );
        res.json({ success: true, id: result.rows[0].id });
    } catch (err) {
        if (err.code === '23505') { // Unique violation
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
        if (err.code === '23505') { // Unique violation
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
        // Check if this is the last admin
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

        const result = await pool.query('DELETE FROM users WHERE id = $1', [id]);
        
        if (result.rowCount === 0) {
            return res.status(404).json({ error: 'User not found' });
        }

        res.json({ success: true });
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

// Settings endpoints
app.get('/api/settings', async (req, res) => {
    try {
        const result = await pool.query('SELECT key, value FROM settings');
        const settings = {};
        result.rows.forEach(row => {
            settings[row.key] = row.value;
        });
        res.json(settings);
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

app.put('/api/settings', async (req, res) => {
    const settings = req.body;

    try {
        for (const [key, value] of Object.entries(settings)) {
            await pool.query(
                `INSERT INTO settings (key, value) VALUES ($1, $2) 
                 ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = CURRENT_TIMESTAMP`,
                [key, value]
            );
        }
        res.json({ success: true });
    } catch (err) {
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
});

// Authentication endpoint
app.post('/api/login', async (req, res) => {
    const { username, password } = req.body;

    if (!username || !password) {
        return res.status(400).json({ error: 'Username and password required' });
    }

    try {
        const result = await pool.query(
            'SELECT * FROM users WHERE username = $1 OR email = $1',
            [username]
        );

        if (result.rows.length === 0) {
            return res.status(401).json({ error: 'Invalid credentials' });
        }

        const user = result.rows[0];
        const validPassword = await bcrypt.compare(password, user.password);
        
        if (!validPassword) {
            return res.status(401).json({ error: 'Invalid credentials' });
        }

        // Update last login
        await pool.query(
            'UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = $1',
            [user.id]
        );

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
        console.error('Database error:', err);
        res.status(500).json({ error: 'Database error' });
    }
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
});

// Graceful shutdown
process.on('SIGINT', () => {
    console.log('\nShutting down gracefully...');
    pool.end(() => {
        console.log('Database pool closed.');
        process.exit(0);
    });
});
