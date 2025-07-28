const cron = require('node-cron');
const db = require('./db');
const { publishToExchange } = require('./rabbitmq');

// This function will fetch all meter readings and send them to RabbitMQ
async function sendAllMeterReadingsToRabbitMQ() {
    try {
        const query = `
            SELECT 
                mr.row_id, 
                mr.meter_number, 
                mr.reading_time, 
                mr.current_reading, 
                mr.previous_reading, 
                mr.created_on,
                ca.consumer_number,
                u.user_id,
                pb.tariff_rate,
                pb.current_balance,
                pb.balance_id
            FROM cis.meter_readings mr
            LEFT JOIN cis.consumer_accounts ca ON mr.meter_number = ca.meter_number
            LEFT JOIN cis.user u ON ca.consumer_number = u.user_id
            LEFT JOIN cis.prepaid_balance pb ON ca.consumer_number = pb.consumer_number
            WHERE u.user_id IS NOT NULL AND pb.balance_id IS NOT NULL
        `;
        const { rows } = await db.query(query);
        const now = new Date();

        for (const reading of rows) {
            const readingTime = new Date(reading.reading_time);
            const hoursElapsed = Math.max(1, Math.floor((now - readingTime) / (1000 * 60 * 60)));
            const randomIncrement = Math.floor(Math.random() * hoursElapsed);

            const prevCurrentReading = parseFloat(reading.current_reading);
            if (isNaN(prevCurrentReading)) {
                console.warn(`Invalid current_reading for meter ${reading.meter_number}, skipping...`);
                continue;
            }

            const newCurrentReading = prevCurrentReading + randomIncrement;

            // Calculate the charge and update prepaid balance
            const tariffRate = parseFloat(reading.tariff_rate) || 0;
            const charge = randomIncrement * tariffRate;
            const currentBalance = parseFloat(reading.current_balance) || 0;
            const newBalance = Math.max(0, currentBalance - charge); // Ensure balance doesn't go negative

            // Update the database with the new readings
            const updateQuery = `UPDATE cis.meter_readings SET previous_reading = $1, current_reading = $2, reading_time = $3 WHERE row_id = $4`;
            await db.query(updateQuery, [prevCurrentReading, newCurrentReading, now, reading.row_id]);

            // Update the prepaid balance
            const balanceUpdateQuery = `UPDATE cis.prepaid_balance SET current_balance = $1 WHERE balance_id = $2`;
            await db.query(balanceUpdateQuery, [newBalance, reading.balance_id]);

            // Prepare the updated reading object
            /*const updatedReading = {
                ...reading,
                previous_reading: prevCurrentReading,
                current_reading: newCurrentReading,
                reading_time: now,
                consumer_number: reading.consumer_number,
                user_id: reading.user_id,
                tariff_rate: reading.tariff_rate,
                charge: charge,
                previous_balance: currentBalance,
                current_balance: newBalance
            };*/
            const updatedReading = {
                ...reading,
                previous_reading: prevCurrentReading,
                current_reading: newCurrentReading,
                reading_time: now
            };
            await publishToExchange('comm.ex.1', 'meterredingkey', updatedReading);
        }
        console.log(`All ${rows.length} meter readings updated and sent to RabbitMQ`);
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
