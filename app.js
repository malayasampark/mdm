require('./scheduler'); // Start the prepaid meter scheduler
require('./billScheduler'); // Start the bill generation scheduler
require('dotenv').config(); // Load environment variables from .env file

const fs = require('fs'); // For file system operations
const db = require('./db'); // Assuming db.js is in the same directory

const express = require('express');
const meterReadingsRoutes = require('./meterReadingsRoutes');

const app = express();
app.use(express.json());
app.use('/', meterReadingsRoutes);

const PORT = process.env.PORT || 3100;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
