// Customer management endpoint - Add this to your server.js file

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

// Get customers with excluded status
app.get('/api/customers-with-status', async (req, res) => {
  try {
    const query = `
      SELECT 
        r.customer_name,
        r.country,
        r.city,
        COUNT(DISTINCT r.order_reference) as order_count,
        SUM(r.quantity) as total_quantity,
        SUM(r.total) as total_revenue,
        MAX(r.date) as last_order_date,
        CASE 
          WHEN ec.customer_name IS NOT NULL THEN true 
          ELSE false 
        END as is_excluded
      FROM records r
      LEFT JOIN excluded_customers ec ON r.customer_name = ec.customer_name
      WHERE r.customer_name IS NOT NULL
      GROUP BY r.customer_name, r.country, r.city, ec.customer_name
      ORDER BY r.customer_name
    `;
    
    const result = await pool.query(query);
    res.json(result.rows);
  } catch (error) {
    console.error('Error fetching customers with status:', error);
    res.status(500).json({ error: 'Failed to fetch customers' });
  }
});

// Exclude a customer
app.post('/api/customers/exclude', async (req, res) => {
  const { customerName } = req.body;
  
  try {
    // First check if already excluded
    const checkQuery = 'SELECT * FROM excluded_customers WHERE customer_name = $1';
    const checkResult = await pool.query(checkQuery, [customerName]);
    
    if (checkResult.rows.length === 0) {
      // Add to excluded list
      const insertQuery = 'INSERT INTO excluded_customers (customer_name) VALUES ($1)';
      await pool.query(insertQuery, [customerName]);
    }
    
    res.json({ message: 'Customer excluded successfully' });
  } catch (error) {
    console.error('Error excluding customer:', error);
    res.status(500).json({ error: 'Failed to exclude customer' });
  }
});

// Include a customer (remove from excluded list)
app.post('/api/customers/include', async (req, res) => {
  const { customerName } = req.body;
  
  try {
    const deleteQuery = 'DELETE FROM excluded_customers WHERE customer_name = $1';
    await pool.query(deleteQuery, [customerName]);
    
    res.json({ message: 'Customer included successfully' });
  } catch (error) {
    console.error('Error including customer:', error);
    res.status(500).json({ error: 'Failed to include customer' });
  }
});

// Bulk exclude customers
app.post('/api/customers/bulk-exclude', async (req, res) => {
  const { customerNames } = req.body;
  
  if (!Array.isArray(customerNames) || customerNames.length === 0) {
    return res.status(400).json({ error: 'Invalid customer names array' });
  }
  
  try {
    // Begin transaction
    await pool.query('BEGIN');
    
    for (const customerName of customerNames) {
      // Check if already excluded
      const checkQuery = 'SELECT * FROM excluded_customers WHERE customer_name = $1';
      const checkResult = await pool.query(checkQuery, [customerName]);
      
      if (checkResult.rows.length === 0) {
        // Add to excluded list
        const insertQuery = 'INSERT INTO excluded_customers (customer_name) VALUES ($1)';
        await pool.query(insertQuery, [customerName]);
      }
    }
    
    await pool.query('COMMIT');
    res.json({ message: `${customerNames.length} customers excluded successfully` });
  } catch (error) {
    await pool.query('ROLLBACK');
    console.error('Error bulk excluding customers:', error);
    res.status(500).json({ error: 'Failed to exclude customers' });
  }
});

// Bulk include customers
app.post('/api/customers/bulk-include', async (req, res) => {
  const { customerNames } = req.body;
  
  if (!Array.isArray(customerNames) || customerNames.length === 0) {
    return res.status(400).json({ error: 'Invalid customer names array' });
  }
  
  try {
    const placeholders = customerNames.map((_, i) => `$${i + 1}`).join(',');
    const deleteQuery = `DELETE FROM excluded_customers WHERE customer_name IN (${placeholders})`;
    await pool.query(deleteQuery, customerNames);
    
    res.json({ message: `${customerNames.length} customers included successfully` });
  } catch (error) {
    console.error('Error bulk including customers:', error);
    res.status(500).json({ error: 'Failed to include customers' });
  }
});

// Also ensure the excluded_customers table exists in your initDatabase function:
async function initDatabase() {
  try {
    // ... existing table creation code ...

    // Create excluded_customers table if it doesn't exist
    await pool.query(`
      CREATE TABLE IF NOT EXISTS excluded_customers (
        id SERIAL PRIMARY KEY,
        customer_name VARCHAR(255) UNIQUE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    console.log('Database initialized successfully');
  } catch (error) {
    console.error('Error initializing database:', error);
  }
}
