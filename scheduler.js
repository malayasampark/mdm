const cron = require('node-cron');
const db = require('./db');
const { publishToExchange } = require('./rabbitmq');

// This function will fetch all meter readings and send them to RabbitMQ
async function sendAllMeterReadingsToRabbitMQ() {
    try {
        const query = `SELECT row_id, meter_number, reading_time, current_reading, previous_reading, created_on FROM cis.meter_readings`;
        const { rows } = await db.query(query);
        for (const reading of rows) {
            await publishToExchange('comm.ex.1', 'consumerkey', { meter_number: reading.meter_number, reading });
        }
        console.log('All meter readings sent to RabbitMQ');
    } catch (err) {
        console.error('Error sending meter readings to RabbitMQ:', err);
    }
}

// Schedule the task to run every day at 1:00 AM
cron.schedule('0 1 * * *', () => {
    console.log('Scheduler running: sending all meter readings to RabbitMQ');
    sendAllMeterReadingsToRabbitMQ();
});

module.exports = {
    sendAllMeterReadingsToRabbitMQ
};
