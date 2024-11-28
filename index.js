const { Kafka } = require('kafkajs');  // KafkaJS
const express = require('express');
const http = require('http');
const socketIo = require('socket.io');

// Create an Express app and HTTP server
const app = express();
const server = http.createServer(app);
const io = socketIo(server);

// Initialize Kafka producer
const kafka = new Kafka({
  clientId: 'socketio-server',  // Unique client ID
  brokers: ['localhost:9092'],   // Kafka brokers
});

const producer = kafka.producer();

// Connect Kafka producer
async function connectProducer() {
  await producer.connect();
  console.log('Kafka Producer connected');
}

// Function to send message to Kafka topic
async function sendToKafka(message) {
  try {
    await producer.send({
      topic: 'messages',  // Kafka topic
      messages: [
        { value: message },  // Actual message
      ],
    });
    console.log('Message sent to Kafka:', message);
  } catch (err) {
    console.error('Error sending message to Kafka:', err);
  }
}

// Listen for client connections
io.on('connection', (socket) => {
  console.log('A user connected:', socket.id);

  // Listen for "chat message" event from client
  socket.on('chat message', (msg) => {
    console.log('Received message from client:', msg);

    // Send the message to Kafka
    sendToKafka(msg);  // This will forward the message to Kafka topic

    // Send a confirmation to the client
    socket.emit('message status', 'Message received and sent to Kafka');
  });

  // Listen for user disconnect
  socket.on('disconnect', () => {
    console.log('A user disconnected:', socket.id);
  });
});

// Start the server
server.listen(3000, () => {
  console.log('Server is running on port 3000');
  connectProducer();  // Connect the Kafka producer when the server starts
});
