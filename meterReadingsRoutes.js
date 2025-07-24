const express = require('express');
const db = require('./db');

const { publishToExchange } = require('./rabbitmq');

const router = express.Router();

// GET /getMeterReadings?meter_number=xxxx
router.get('/getMeterReadings', async (req, res) => {
    const meterNumber = req.query.meter_number;
    if (!meterNumber) {
        return res.status(400).json({ error: 'meter_number is required' });
    }

    console.log('Environment check:', {
        DB_HOST: process.env.DB_HOST,
        DB_USER: process.env.DB_USER,
        DB_NAME: process.env.DB_NAME,
        DB_PORT: process.env.DB_PORT
    });

    try {
        const query = `SELECT row_id, meter_number, reading_time, current_reading, previous_reading, created_on FROM cis.meter_readings WHERE meter_number = $1 ORDER BY reading_time DESC LIMIT 1`;
        const { rows } = await db.query(query, [meterNumber]);
        console.log('Fetched meter readings:', rows);
        if (rows.length === 0) {
            return res.status(404).json({ error: 'No readings found for this meter_number' });
        }

        const reading = rows[0];
        const now = new Date();
        const readingTime = new Date(reading.reading_time);
        console.log('Current reading time:', readingTime);

        const hoursElapsed = Math.max(1, Math.floor((now - readingTime) / (1000 * 60 * 60)));
        const randomIncrement = Math.floor(Math.random() * hoursElapsed);

        // Ensure current_reading is a valid number
        const prevCurrentReading = parseFloat(reading.current_reading);
        if (isNaN(prevCurrentReading)) {
            return res.status(400).json({ error: 'Invalid current_reading value in database' });
        }

        const newCurrentReading = prevCurrentReading + randomIncrement;

        // Update the database with the new readings
        const updateQuery = `UPDATE cis.meter_readings SET previous_reading = $1, current_reading = $2, reading_time = $3 WHERE row_id = $4`;
        await db.query(updateQuery, [prevCurrentReading, newCurrentReading, now, reading.row_id]);

        // Prepare the response object
        const updatedReading = {
            ...reading,
            previous_reading: prevCurrentReading,
            current_reading: newCurrentReading,
            reading_time: now
        };

        // Publish to RabbitMQ exchange
        try {
            await publishToExchange('comm.ex.1', 'meterredingkey', updatedReading);
        } catch (rabbitErr) {
            console.error('Error publishing to RabbitMQ:', rabbitErr);
        }

        return res.json(updatedReading);
    } catch (err) {
        console.error('Error fetching or updating meter readings:', err);
        return res.status(500).json({ error: 'Internal server error' });
    }
});

module.exports = router;
