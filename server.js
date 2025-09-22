const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const multer = require('multer');
const xlsx = require('xlsx');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');
const bcrypt = require('bcryptjs');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(express.static('public'));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Configure multer for file uploads
const upload = multer({ dest: 'uploads/' });

// Database setup
const db = new sqlite3.Database('./stockist.db');

// Create tables if they don't exist
db.serialize(() => {
    // Main stockist data table
    db.run(`CREATE TABLE IF NOT EXISTS stockist_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_name TEXT,
        display_name TEXT,
        location TEXT,
        quantity INTEGER,
        order_date TEXT,
        order_reference TEXT UNIQUE,
        item_name TEXT,
        variant_name TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);

    // Customer name mappings table
    db.run(`CREATE TABLE IF NOT EXISTS customer_mappings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        original_name TEXT UNIQUE,
        display_name TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);

    // Users table
    db.run(`CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE,
        email TEXT UNIQUE,
        password TEXT,
        role TEXT DEFAULT 'viewer',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        last_login DATETIME
    )`);

    // Settings table
    db.run(`CREATE TABLE IF NOT EXISTS settings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        key TEXT UNIQUE,
        value TEXT,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);

    // Excluded customers table
    db.run(`CREATE TABLE IF NOT EXISTS excluded_customers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_name TEXT UNIQUE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);

    // Create default admin user if no users exist
    db.get("SELECT COUNT(*) as count FROM users", [], (err, row) => {
        if (row && row.count === 0) {
            const defaultPassword = bcrypt.hashSync('admin123', 10);
            db.run(`INSERT INTO users (username, email, password, role) VALUES (?, ?, ?, ?)`,
                ['admin', 'admin@antennebooks.com', defaultPassword, 'admin'],
                (err) => {
                    if (!err) {
                        console.log('Default admin user created (username: admin, password: admin123)');
                    }
                });
        }
    });
});

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
    const fileType = req.body.fileType || 'auto';
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
                        const mapping = await new Promise((resolve) => {
                            db.get(
                                "SELECT display_name FROM customer_mappings WHERE original_name = ?",
                                [customer],
                                (err, row) => resolve(row)
                            );
                        });

                        const displayName = mapping ? mapping.display_name : customer;

                        await new Promise((resolve, reject) => {
                            db.run(
                                `INSERT OR IGNORE INTO stockist_data 
                                (customer_name, display_name, quantity, order_date, order_reference, item_name, variant_name) 
                                VALUES (?, ?, ?, ?, ?, ?, ?)`,
                                [customer, displayName, quantity, orderDate, orderRef, itemName, variantName],
                                function(err) {
                                    if (err) reject(err);
                                    else if (this.changes > 0) {
                                        successCount++;
                                        resolve();
                                    } else {
                                        duplicateCount++;
                                        resolve();
                                    }
                                }
                            );
                        });
                    } catch (error) {
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
                        const mapping = await new Promise((resolve) => {
                            db.get(
                                "SELECT display_name FROM customer_mappings WHERE original_name = ?",
                                [customer],
                                (err, row) => resolve(row)
                            );
                        });

                        const displayName = mapping ? mapping.display_name : customer;

                        await new Promise((resolve, reject) => {
                            db.run(
                                `INSERT OR IGNORE INTO stockist_data 
                                (customer_name, display_name, location, quantity, order_date, order_reference, item_name, variant_name) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
                                [customer, displayName, location, quantity, orderDate, orderRef, itemName, variantName],
                                function(err) {
                                    if (err) reject(err);
                                    else if (this.changes > 0) {
                                        successCount++;
                                        resolve();
                                    } else {
                                        duplicateCount++;
                                        resolve();
                                    }
                                }
                            );
                        });
                    } catch (error) {
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
        fs.unlinkSync(filePath);
        res.status(500).json({ error: 'Failed to process file' });
    }
});

// Get customers endpoint
app.get('/api/customers', (req, res) => {
    const search = req.query.search || '';
    const excluded = req.query.excluded === 'true';

    let query = `
        SELECT DISTINCT 
            COALESCE(display_name, customer_name) as display_name,
            customer_name,
            location,
            COUNT(*) as order_count,
            SUM(quantity) as total_quantity,
            CASE WHEN ec.customer_name IS NOT NULL THEN 1 ELSE 0 END as is_excluded
        FROM stockist_data sd
        LEFT JOIN excluded_customers ec ON sd.customer_name = ec.customer_name
    `;

    const params = [];
    if (search) {
        query += ` WHERE (sd.customer_name LIKE ? OR sd.display_name LIKE ? OR sd.location LIKE ?)`;
        params.push(`%${search}%`, `%${search}%`, `%${search}%`);
    }

    query += ` GROUP BY sd.customer_name, sd.display_name, sd.location`;

    if (!excluded) {
        query = `SELECT * FROM (${query}) WHERE is_excluded = 0`;
    }

    query += ` ORDER BY display_name`;

    db.all(query, params, (err, rows) => {
        if (err) {
            console.error('Database error:', err);
            res.status(500).json({ error: 'Database error' });
        } else {
            res.json(rows);
        }
    });
});

// Update customer location
app.put('/api/customers/:name/location', (req, res) => {
    const { name } = req.params;
    const { location } = req.body;

    db.run(
        "UPDATE stockist_data SET location = ? WHERE customer_name = ?",
        [location, name],
        function(err) {
            if (err) {
                res.status(500).json({ error: 'Database error' });
            } else {
                res.json({ success: true, changes: this.changes });
            }
        }
    );
});

// Exclude/include customer
app.post('/api/customers/:name/exclude', (req, res) => {
    const { name } = req.params;

    db.run(
        "INSERT OR IGNORE INTO excluded_customers (customer_name) VALUES (?)",
        [name],
        (err) => {
            if (err) {
                res.status(500).json({ error: 'Database error' });
            } else {
                res.json({ success: true });
            }
        }
    );
});

app.delete('/api/customers/:name/exclude', (req, res) => {
    const { name } = req.params;

    db.run(
        "DELETE FROM excluded_customers WHERE customer_name = ?",
        [name],
        (err) => {
            if (err) {
                res.status(500).json({ error: 'Database error' });
            } else {
                res.json({ success: true });
            }
        }
    );
});

// Get unique titles
app.get('/api/titles', (req, res) => {
    db.all(
        "SELECT DISTINCT item_name FROM stockist_data WHERE item_name IS NOT NULL AND item_name != '' ORDER BY item_name",
        [],
        (err, rows) => {
            if (err) {
                res.status(500).json({ error: 'Database error' });
            } else {
                res.json(rows.map(row => row.item_name));
            }
        }
    );
});

// Generate report
app.post('/api/generate-report', (req, res) => {
    const { titles, startDate, endDate, format, excludeNoLocation } = req.body;

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

    if (titles && titles.length > 0) {
        query += ` AND sd.item_name IN (${titles.map(() => '?').join(',')})`;
        params.push(...titles);
    }

    if (startDate) {
        query += ` AND date(sd.order_date) >= date(?)`;
        params.push(startDate);
    }

    if (endDate) {
        query += ` AND date(sd.order_date) <= date(?)`;
        params.push(endDate);
    }

    if (excludeNoLocation) {
        query += ` AND sd.location IS NOT NULL AND sd.location != ''`;
    }

    query += ` GROUP BY sd.customer_name, sd.display_name, sd.location, sd.item_name, sd.variant_name
               ORDER BY customer_name, sd.item_name`;

    db.all(query, params, (err, rows) => {
        if (err) {
            console.error('Database error:', err);
            res.status(500).json({ error: 'Database error' });
        } else {
            res.json(rows);
        }
    });
});

// Customer name mappings endpoints
app.get('/api/mappings', (req, res) => {
    db.all(
        "SELECT * FROM customer_mappings ORDER BY original_name",
        [],
        (err, rows) => {
            if (err) {
                res.status(500).json({ error: 'Database error' });
            } else {
                res.json(rows);
            }
        }
    );
});

app.post('/api/mappings', (req, res) => {
    const { original_name, display_name } = req.body;

    if (!original_name || !display_name) {
        return res.status(400).json({ error: 'Both original_name and display_name are required' });
    }

    db.run(
        "INSERT OR REPLACE INTO customer_mappings (original_name, display_name) VALUES (?, ?)",
        [original_name, display_name],
        function(err) {
            if (err) {
                res.status(500).json({ error: 'Database error' });
            } else {
                // Update existing records with the new display name
                db.run(
                    "UPDATE stockist_data SET display_name = ? WHERE customer_name = ?",
                    [display_name, original_name],
                    (err) => {
                        if (err) {
                            console.error('Error updating existing records:', err);
                        }
                    }
                );
                res.json({ success: true, id: this.lastID });
            }
        }
    );
});

app.delete('/api/mappings/:id', (req, res) => {
    const { id } = req.params;

    // First get the mapping to know which records to update
    db.get("SELECT original_name FROM customer_mappings WHERE id = ?", [id], (err, mapping) => {
        if (err || !mapping) {
            return res.status(500).json({ error: 'Database error' });
        }

        // Delete the mapping
        db.run("DELETE FROM customer_mappings WHERE id = ?", [id], (err) => {
            if (err) {
                res.status(500).json({ error: 'Database error' });
            } else {
                // Reset display names to original names
                db.run(
                    "UPDATE stockist_data SET display_name = customer_name WHERE customer_name = ?",
                    [mapping.original_name],
                    (err) => {
                        if (err) {
                            console.error('Error resetting display names:', err);
                        }
                    }
                );
                res.json({ success: true });
            }
        });
    });
});

// Data management endpoints
app.get('/api/stats', (req, res) => {
    db.get(
        `SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT customer_name) as total_customers,
            COUNT(DISTINCT item_name) as total_titles
         FROM stockist_data`,
        [],
        (err, row) => {
            if (err) {
                res.status(500).json({ error: 'Database error' });
            } else {
                res.json(row);
            }
        }
    );
});

app.get('/api/export-all', (req, res) => {
    db.all(
        `SELECT * FROM stockist_data ORDER BY order_date DESC`,
        [],
        (err, rows) => {
            if (err) {
                res.status(500).json({ error: 'Database error' });
            } else {
                // Convert to CSV
                const headers = Object.keys(rows[0] || {}).join(',');
                const csvData = rows.map(row => 
                    Object.values(row).map(val => 
                        typeof val === 'string' && val.includes(',') ? `"${val}"` : val
                    ).join(',')
                ).join('\n');
                
                res.setHeader('Content-Type', 'text/csv');
                res.setHeader('Content-Disposition', 'attachment; filename=stockist_data.csv');
                res.send(headers + '\n' + csvData);
            }
        }
    );
});

app.post('/api/backup', (req, res) => {
    const backupFile = `backup_${Date.now()}.db`;
    const backupPath = path.join(__dirname, 'backups', backupFile);
    
    // Create backups directory if it doesn't exist
    if (!fs.existsSync(path.join(__dirname, 'backups'))) {
        fs.mkdirSync(path.join(__dirname, 'backups'));
    }

    // Copy database file
    fs.copyFile('./stockist.db', backupPath, (err) => {
        if (err) {
            res.status(500).json({ error: 'Backup failed' });
        } else {
            res.json({ success: true, backup: backupFile });
        }
    });
});

app.delete('/api/clear-data', (req, res) => {
    db.run("DELETE FROM stockist_data", function(err) {
        if (err) {
            res.status(500).json({ error: 'Database error' });
        } else {
            res.json({ success: true, deleted: this.changes });
        }
    });
});

app.delete('/api/remove-old-records', (req, res) => {
    const sixMonthsAgo = new Date();
    sixMonthsAgo.setMonth(sixMonthsAgo.getMonth() - 6);
    
    db.run(
        "DELETE FROM stockist_data WHERE date(order_date) < date(?)",
        [sixMonthsAgo.toISOString()],
        function(err) {
            if (err) {
                res.status(500).json({ error: 'Database error' });
            } else {
                res.json({ success: true, deleted_count: this.changes });
            }
        }
    );
});

app.post('/api/merge-duplicates', (req, res) => {
    // This is a simplified merge - you might want to add more sophisticated logic
    db.run(
        `UPDATE stockist_data 
         SET display_name = customer_name 
         WHERE display_name IS NULL OR display_name = ''`,
        function(err) {
            if (err) {
                res.status(500).json({ error: 'Database error' });
            } else {
                res.json({ success: true, merged_count: this.changes });
            }
        }
    );
});

// User management endpoints
app.get('/api/users', (req, res) => {
    db.all(
        "SELECT id, username, email, role, created_at, last_login FROM users",
        [],
        (err, rows) => {
            if (err) {
                res.status(500).json({ error: 'Database error' });
            } else {
                res.json(rows);
            }
        }
    );
});

app.get('/api/users/:id', (req, res) => {
    const { id } = req.params;
    
    db.get(
        "SELECT id, username, email, role, created_at, last_login FROM users WHERE id = ?",
        [id],
        (err, row) => {
            if (err) {
                res.status(500).json({ error: 'Database error' });
            } else if (!row) {
                res.status(404).json({ error: 'User not found' });
            } else {
                res.json(row);
            }
        }
    );
});

app.post('/api/users', async (req, res) => {
    const { username, email, password, role } = req.body;

    if (!username || !email || !password) {
        return res.status(400).json({ error: 'Username, email, and password are required' });
    }

    try {
        // Hash the password
        const hashedPassword = await bcrypt.hash(password, 10);

        db.run(
            "INSERT INTO users (username, email, password, role) VALUES (?, ?, ?, ?)",
            [username, email, hashedPassword, role || 'viewer'],
            function(err) {
                if (err) {
                    if (err.message.includes('UNIQUE')) {
                        res.status(400).json({ error: 'Username or email already exists' });
                    } else {
                        res.status(500).json({ error: 'Database error' });
                    }
                } else {
                    res.json({ success: true, id: this.lastID });
                }
            }
        );
    } catch (error) {
        res.status(500).json({ error: 'Failed to create user' });
    }
});

app.put('/api/users/:id', async (req, res) => {
    const { id } = req.params;
    const { username, email, password, role } = req.body;

    let query = "UPDATE users SET username = ?, email = ?, role = ?";
    let params = [username, email, role];

    if (password) {
        const hashedPassword = await bcrypt.hash(password, 10);
        query += ", password = ?";
        params.push(hashedPassword);
    }

    query += " WHERE id = ?";
    params.push(id);

    db.run(query, params, function(err) {
        if (err) {
            if (err.message.includes('UNIQUE')) {
                res.status(400).json({ error: 'Username or email already exists' });
            } else {
                res.status(500).json({ error: 'Database error' });
            }
        } else if (this.changes === 0) {
            res.status(404).json({ error: 'User not found' });
        } else {
            res.json({ success: true });
        }
    });
});

app.delete('/api/users/:id', (req, res) => {
    const { id } = req.params;

    // Prevent deleting the last admin
    db.get(
        "SELECT COUNT(*) as admin_count FROM users WHERE role = 'admin' AND id != ?",
        [id],
        (err, row) => {
            if (err) {
                return res.status(500).json({ error: 'Database error' });
            }

            db.get("SELECT role FROM users WHERE id = ?", [id], (err, user) => {
                if (err) {
                    return res.status(500).json({ error: 'Database error' });
                }

                if (user && user.role === 'admin' && row.admin_count === 0) {
                    return res.status(400).json({ error: 'Cannot delete the last admin user' });
                }

                db.run("DELETE FROM users WHERE id = ?", [id], function(err) {
                    if (err) {
                        res.status(500).json({ error: 'Database error' });
                    } else if (this.changes === 0) {
                        res.status(404).json({ error: 'User not found' });
                    } else {
                        res.json({ success: true });
                    }
                });
            });
        }
    );
});

// Settings endpoints
app.get('/api/settings', (req, res) => {
    db.all("SELECT key, value FROM settings", [], (err, rows) => {
        if (err) {
            res.status(500).json({ error: 'Database error' });
        } else {
            const settings = {};
            rows.forEach(row => {
                settings[row.key] = row.value;
            });
            res.json(settings);
        }
    });
});

app.put('/api/settings', (req, res) => {
    const settings = req.body;
    const stmt = db.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)");

    for (const [key, value] of Object.entries(settings)) {
        stmt.run(key, value);
    }

    stmt.finalize((err) => {
        if (err) {
            res.status(500).json({ error: 'Database error' });
        } else {
            res.json({ success: true });
        }
    });
});

// Bulk location update endpoint
app.post('/api/bulk-update-locations', upload.single('file'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'No file uploaded' });
    }

    const filePath = req.file.path;
    let updateCount = 0;
    let errorCount = 0;

    try {
        const results = [];
        await new Promise((resolve, reject) => {
            fs.createReadStream(filePath)
                .pipe(csv())
                .on('data', (data) => results.push(data))
                .on('end', () => resolve())
                .on('error', reject);
        });

        for (const row of results) {
            const customerName = row['Customer Name'] || row['customer_name'] || '';
            const location = row['Location'] || row['location'] || '';

            if (customerName && location) {
                await new Promise((resolve) => {
                    db.run(
                        "UPDATE stockist_data SET location = ? WHERE customer_name = ?",
                        [location, customerName],
                        function(err) {
                            if (!err && this.changes > 0) {
                                updateCount++;
                            } else {
                                errorCount++;
                            }
                            resolve();
                        }
                    );
                });
            }
        }

        fs.unlinkSync(filePath);
        res.json({
            success: true,
            message: `Updated ${updateCount} customer locations`,
            stats: { updated: updateCount, errors: errorCount }
        });

    } catch (error) {
        console.error('Bulk update error:', error);
        fs.unlinkSync(filePath);
        res.status(500).json({ error: 'Failed to process file' });
    }
});

// Authentication endpoints (basic implementation - you should enhance this)
app.post('/api/login', async (req, res) => {
    const { username, password } = req.body;

    if (!username || !password) {
        return res.status(400).json({ error: 'Username and password required' });
    }

    db.get(
        "SELECT * FROM users WHERE username = ? OR email = ?",
        [username, username],
        async (err, user) => {
            if (err) {
                return res.status(500).json({ error: 'Database error' });
            }

            if (!user) {
                return res.status(401).json({ error: 'Invalid credentials' });
            }

            const validPassword = await bcrypt.compare(password, user.password);
            if (!validPassword) {
                return res.status(401).json({ error: 'Invalid credentials' });
            }

            // Update last login
            db.run(
                "UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = ?",
                [user.id]
            );

            // In production, you should use proper session management or JWT tokens
            res.json({
                success: true,
                user: {
                    id: user.id,
                    username: user.username,
                    email: user.email,
                    role: user.role
                }
            });
        }
    );
});

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({ status: 'healthy', timestamp: new Date().toISOString() });
});

// Start server
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
    console.log(`Visit http://localhost:${PORT} to access the application`);
});

// Graceful shutdown
process.on('SIGINT', () => {
    console.log('\nShutting down gracefully...');
    db.close((err) => {
        if (err) {
            console.error(err.message);
        }
        console.log('Database connection closed.');
        process.exit(0);
    });
});
