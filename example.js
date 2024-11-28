const io = require('socket.io-client');

const socketProducer = io('http://localhost:3000'); // Connect to the producer
const socketConsumer = io('http://localhost:3001'); // Connect to the consumer

// Send a message to the producer
socketProducer.emit('chat message', 'Hello, Kafka!');

// Listen for Kafka messages from the consumer
socketConsumer.on('kafka message', (msg) => {
  console.log('Message from Kafka:', msg);
});
