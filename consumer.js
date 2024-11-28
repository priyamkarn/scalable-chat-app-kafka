const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Kafka } = require('kafkajs');

// Express and Socket.IO setup
const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.get('/', (req, res) => {
  res.send('Socket.IO and Kafka integration server is running');
});

// Kafka setup
const kafka = new Kafka({
  clientId: 'socketio-consumer',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'chat-group' });

// Kafka consumer function
async function connectConsumer() {
  await consumer.connect();
  console.log('Kafka Consumer connected');

  await consumer.subscribe({ topic: 'messages', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const receivedMessage = message.value.toString();
      console.log('Consumed message:', receivedMessage);

      // Broadcast message to all connected clients
      io.emit('broadcast message', receivedMessage);
    },
  });
}

// Handle client connections
io.on('connection', (socket) => {
  console.log('A client connected:', socket.id);

  socket.on('disconnect', () => {
    console.log('A client disconnected:', socket.id);
  });
});

// Start the server and Kafka consumer
server.listen(3001, () => {
  console.log('Server running on port 3001');
  connectConsumer();
});
