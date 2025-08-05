const amqp = require('amqplib');

let channel = null;
let connection = null;

async function connectRabbitMQ() {
    try {
        if (channel && connection && !connection.connection.destroyed) {
            return { channel, connection };
        }

        const rabbitUrl = `amqp://${process.env.RABBITMQ_USER}:${process.env.RABBITMQ_PASSWORD}@${process.env.RABBITMQ_HOST}:${process.env.RABBITMQ_PORT}/${process.env.RABBITMQ_VHOST || 'edfvhost'}`;
        console.log('Connecting to RabbitMQ:', `amqp://${process.env.RABBITMQ_USER}:****@${process.env.RABBITMQ_HOST}:${process.env.RABBITMQ_PORT}/${process.env.RABBITMQ_VHOST || 'edfvhost'}`);

        connection = await amqp.connect(rabbitUrl);
        console.log('RabbitMQ connection established');

        connection.on('error', (err) => {
            console.error('RabbitMQ connection error:', err);
            channel = null;
            connection = null;
        });

        connection.on('close', () => {
            console.log('RabbitMQ connection closed');
            channel = null;
            connection = null;
        });

        channel = await connection.createChannel();
        console.log('RabbitMQ channel created');

        return { channel, connection };
    } catch (error) {
        console.error('Failed to connect to RabbitMQ:', error.message);
        throw error;
    }
}

async function publishToExchange(exchange, routingKey, payload) {
    try {
        const { channel } = await connectRabbitMQ();
        await channel.assertExchange(exchange, 'direct', { durable: true });
        const result = channel.publish(exchange, routingKey, Buffer.from(JSON.stringify(payload)));
        return result;
    } catch (error) {
        console.error('Failed to publish message to RabbitMQ:', error.message);
        throw error;
    }
}

async function closeRabbitMQ() {
    if (channel) await channel.close();
    if (connection) await connection.close();
    channel = null;
    connection = null;
}

module.exports = {
    publishToExchange,
    closeRabbitMQ
};
