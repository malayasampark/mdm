const amqp = require('amqplib');

let channel = null;
let connection = null;

async function connectRabbitMQ() {
    if (channel && connection) return { channel, connection };
    const rabbitUrl = `amqp://${process.env.RABBITMQ_USER}:${process.env.RABBITMQ_PASSWORD}@${process.env.RABBITMQ_HOST}:${process.env.RABBITMQ_PORT}`;
    connection = await amqp.connect(rabbitUrl);
    channel = await connection.createChannel();
    return { channel, connection };
}

async function publishToExchange(exchange, routingKey, payload) {
    const { channel } = await connectRabbitMQ();
    await channel.assertExchange(exchange, 'direct', { durable: true });
    channel.publish(exchange, routingKey, Buffer.from(JSON.stringify(payload)));
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
