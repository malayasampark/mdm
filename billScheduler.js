const cron = require('node-cron');
const db = require('./db');
const { publishToExchange } = require('./rabbitmq');

// This function will generate bills for postpaid consumers whose last reading date exceeds 1 month
async function generateMonthlyBills() {
    try {
        // Step 1: Fetch all consumer information with meter if they have logged in
        const query = `
            SELECT 
                mr.meter_number,
                ca.consumer_number,
                ca.connection_type,
                ca.tariff_rate,
                u.user_id,
                mr.reading_time,
                mr.current_reading,
                mr.previous_reading
            FROM cis.meter_readings mr
            LEFT JOIN cis.consumer_accounts ca ON mr.meter_number = ca.meter_number
            LEFT JOIN cis.users u ON ca.consumer_number = u.user_id
            WHERE u.user_id IS NOT NULL 
            AND ca.connection_type = 'Postpaid'
            ORDER BY mr.meter_number, mr.reading_time DESC
        `;

        const { rows } = await db.query(query);
        const now = new Date();
        const processedMeters = new Set();

        console.log(`Found ${rows.length} total records to process`);

        // Step 2: Iterate through each consumer/meter combination
        for (const record of rows) {
            // Skip if we've already processed this meter
            if (processedMeters.has(record.meter_number)) {
                continue;
            }
            processedMeters.add(record.meter_number);

            console.log(`Processing meter: ${record.meter_number} for consumer: ${record.consumer_number}`);

            // Step 3: Check if billing information exists and if bill date is more than 1 month
            const billCheckQuery = `
                SELECT bill_id, bill_period_end 
                FROM cis.bills 
                WHERE meter_number = $1 
                ORDER BY bill_period_end DESC 
                LIMIT 1
            `;

            const billCheckResult = await db.query(billCheckQuery, [record.meter_number]);

            let shouldGenerateBill = false;

            if (billCheckResult.rows.length === 0) {
                // No bills exist for this meter
                shouldGenerateBill = true;
            } else {
                // Check if the last bill is more than 1 month old
                const lastBillDate = new Date(billCheckResult.rows[0].bill_period_end);
                const oneMonthAgo = new Date();
                oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1);

                if (lastBillDate <= oneMonthAgo) {
                    shouldGenerateBill = true;
                }
            }

            if (shouldGenerateBill) {
                // Get the previous bill's current reading to use as previous reading
                const previousBillQuery = `
                    SELECT current_reading 
                    FROM cis.bills 
                    WHERE meter_number = $1 
                    ORDER BY bill_period_end DESC 
                    LIMIT 1
                `;

                const previousBillResult = await db.query(previousBillQuery, [record.meter_number]);

                let previousReading;
                if (previousBillResult.rows.length === 0) {
                    // No previous bill exists, generate random number between 100-200
                    previousReading = Math.floor(Math.random() * 101) + 100; // 100 to 200
                } else {
                    previousReading = parseFloat(previousBillResult.rows[0].current_reading) || 0;
                }

                // Generate current reading: random number between 1-700 + previous reading
                const randomIncrement = Math.floor(Math.random() * 700) + 1; // 1 to 700
                const currentReading = previousReading + randomIncrement;

                const unitsConsumed = currentReading - previousReading;

                // Date calculations
                const billGenerationMonth = new Date(now);

                // bill_period_start: 1st date of the previous month (June 1, 2025)
                const billPeriodStart = new Date(billGenerationMonth.getFullYear(), billGenerationMonth.getMonth() - 1, 1);

                // bill_period_end: last date of the previous month (June 30, 2025)
                const billPeriodEnd = new Date(billGenerationMonth.getFullYear(), billGenerationMonth.getMonth(), 0);

                // reading_date: 1st date of the bill generation month (July 1, 2025)
                const readingDate = new Date(billGenerationMonth.getFullYear(), billGenerationMonth.getMonth(), 1);

                // due_date: 9th of bill generation month (July 9, 2025)
                const dueDate = new Date(billGenerationMonth.getFullYear(), billGenerationMonth.getMonth(), 9);

                console.log(`Date calculations for meter ${record.meter_number}:`, {
                    billPeriodStart: billPeriodStart.toDateString(),
                    billPeriodEnd: billPeriodEnd.toDateString(),
                    readingDate: readingDate.toDateString(),
                    dueDate: dueDate.toDateString()
                });

                // Calculate charges using tariff_rate from consumer_accounts
                const tariffRate = parseFloat(record.tariff_rate);
                if (!tariffRate || isNaN(tariffRate)) {
                    console.warn(`Invalid or missing tariff_rate for meter ${record.meter_number}, skipping...`);
                    continue;
                }

                const energyCharges = unitsConsumed * tariffRate;
                const fixedCharges = 150.00; // Fixed amount of 150
                const taxRate = 0.18; // 18%
                const subsidyRate = 0.05; // 5%

                const taxes = energyCharges * taxRate; // 18% of energy charges only
                const subsidy = energyCharges * subsidyRate; // 5% of energy charges
                const otherCharges = 0;
                const totalAmount = energyCharges + fixedCharges + taxes - subsidy + otherCharges;

                console.log(`Bill calculation for meter ${record.meter_number}:`, {
                    unitsConsumed,
                    tariffRate,
                    energyCharges,
                    fixedCharges,
                    taxes,
                    subsidy,
                    totalAmount
                });

                // Insert bill into database
                const insertBillQuery = `
                    INSERT INTO cis.bills (
                        consumer_number, meter_number, bill_period_start, bill_period_end,
                        reading_date, current_reading, previous_reading, units_consumed,
                        energy_charges, fixed_charges, taxes, subsidy, other_charges,
                        total_amount, due_date, bill_status, created_on
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                `;

                await db.query(insertBillQuery, [
                    record.consumer_number,
                    record.meter_number,
                    billPeriodStart,
                    billPeriodEnd,
                    readingDate,
                    currentReading,
                    previousReading,
                    unitsConsumed,
                    energyCharges,
                    fixedCharges,
                    taxes,
                    subsidy,
                    otherCharges,
                    totalAmount,
                    dueDate,
                    'generated',
                    now
                ]);

                console.log(`Bill generated for consumer: ${record.consumer_number}, meter: ${record.meter_number}, amount: ${totalAmount}`);
            }
        }

        console.log(`Monthly billing completed. ${processedMeters.size} meters processed.`);
    } catch (err) {
        console.error('Error generating monthly bills:', err);
    }
}

// Schedule the task to run every day at 2:00 AM (after meter reading scheduler)
cron.schedule('0 14 * * *', () => {
    console.log('Monthly billing scheduler running...');
    generateMonthlyBills();
});

// Also schedule to run on the 1st of every month at 9:00 AM
cron.schedule('0 9 1 * *', () => {
    console.log('Monthly billing scheduler running (monthly trigger)...');
    generateMonthlyBills();
});

module.exports = {
    generateMonthlyBills
};
