// API endpoint to get customer data with statistics (UPDATED WITH CITY)
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

// API endpoint to update customer information (UPDATED TO HANDLE CITY)
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
