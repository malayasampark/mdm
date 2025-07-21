require('dotenv').config();
const { publishToExchange, closeRabbitMQ } = require('./rabbitmq');

async function testRabbitMQ() {
    try {
        console.log('Testing RabbitMQ connection...');
        console.log('Environment variables:');
        console.log('RABBITMQ_HOST:', process.env.RABBITMQ_HOST);
        console.log('RABBITMQ_PORT:', process.env.RABBITMQ_PORT);
        console.log('RABBITMQ_USER:', process.env.RABBITMQ_USER);

        await publishToExchange('comm.ex.1', 'consumerkey', { test: 'message', timestamp: new Date() });
        console.log('Test message sent successfully!');

        await closeRabbitMQ();
        console.log('Connection closed successfully');
    } catch (error) {
        console.error('RabbitMQ test failed:', error.message);
        console.error('Stack trace:', error.stack);
    }
}

testRabbitMQ();
