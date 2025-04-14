import { ApolloServer } from '@apollo/server';
import { expressMiddleware } from '@apollo/server/express4';
import { createServer } from 'http';
import express from 'express';
import { WebSocketServer } from 'ws';
import { makeExecutableSchema } from '@graphql-tools/schema';
import { PubSub } from 'graphql-subscriptions';
import cors from 'cors';
import bodyParser from 'body-parser';
import { useServer } from 'graphql-ws/use/ws';

// Create PubSub instance for handling real-time updates
const pubsub = new PubSub();

// In-memory data stores
const users = [
  { id: '1', username: 'user1' },
  { id: '2', username: 'user2' }
];

const channels = [
  { id: '1', name: 'general' },
  { id: '2', name: 'random' }
];

const messages = [];

// Define GraphQL schema
const typeDefs = `#graphql
  type User {
    id: ID!
    username: String!
  }

  type Channel {
    id: ID!
    name: String!
  }

  type Message {
    id: ID!
    content: String!
    createdAt: String!
    sender: User!
    channelId: ID!
  }

  type Query {
    users: [User]
    user(id: ID!): User
    channels: [Channel]
    channel(id: ID!): Channel
    messages(channelId: ID!): [Message]
  }

  type Mutation {
    sendMessage(content: String!, senderId: ID!, channelId: ID!): Message
  }

  type Subscription {
    messageAdded(channelId: ID!): Message
  }
`;

// Define resolvers
const resolvers = {
  Query: {
    users: () => users,
    user: (_, { id }) => users.find(user => user.id === id),
    channels: () => channels,
    channel: (_, { id }) => channels.find(channel => channel.id === id),
    messages: (_, { channelId }) => messages.filter(message => message.channelId === channelId)
  },
  Message: {
    sender: (message) => users.find(user => user.id === message.senderId)
  },
  Mutation: {
    sendMessage: (_, { content, senderId, channelId }) => {
      const newMessage = {
        id: String(messages.length + 1),
        content,
        createdAt: new Date().toISOString(),
        senderId,
        channelId
      };
      
      messages.push(newMessage);
      
      const topicName = `MESSAGE_ADDED_${channelId}`;
      pubsub.publish(topicName, { messageAdded: newMessage });
      
      return newMessage;
    }
  },
  Subscription: {
    messageAdded: {
      subscribe: (_, { channelId }) => {
        const topicName = `MESSAGE_ADDED_${channelId}`;
        
        let iterator;
        
        const asyncIterator = {
          [Symbol.asyncIterator]() {
            return {
              next() {
                return new Promise(resolve => {
                  if (iterator) {
                    return iterator;
                  }
                  
                  const listeners = [];
                  
                  let resolveNext;
                  let value;
                  
                  const listener = (event) => {
                    value = event;
                    if (resolveNext) {
                      resolveNext({ value, done: false });
                      resolveNext = null;
                    }
                  };
                  
                  pubsub.subscribe(topicName, listener);
                  listeners.push(listener);
                  
                  if (value) {
                    resolve({ value, done: false });
                    value = null;
                  } else {
                    resolveNext = resolve;
                  }
                });
              },
              return() {
                return Promise.resolve({ value: undefined, done: true });
              }
            };
          }
        };
        
        return asyncIterator;
      }
    }
  }
};

// Create executable schema
const schema = makeExecutableSchema({ typeDefs, resolvers });

async function startServer() {
  // Initialize Express app
  const app = express();
  
  // Create HTTP server
  const httpServer = createServer(app);
  
  // Set up WebSocket server for subscriptions
  const wsServer = new WebSocketServer({
    server: httpServer,
    path: '/graphql',
  });
  
  // Use the schema with the WebSocket server
  const serverCleanup = useServer({ schema }, wsServer);
  
  // Create Apollo Server
  const server = new ApolloServer({
    schema,
    plugins: [{
      async serverWillStart() {
        return {
          async drainServer() {
            await serverCleanup.dispose();
          }
        };
      }
    }],
    formatError: (error) => {
      console.error('GraphQL Error:', error);
      return {
        message: error.message,
        path: error.path,
        extensions: error.extensions
      };
    }
  });
  
  // Start server
  await server.start();
  
  app.use(
    '/graphql',
    cors({
      origin: '*',
      methods: ['GET', 'POST', 'OPTIONS'],
      allowedHeaders: ['Content-Type', 'Authorization']
    }),
    express.json(),
    expressMiddleware(server, {
      context: async ({ req }) => {
        return { 
          token: req.headers.authorization 
        };
      }
    })
  );
  
  app.get('/', (req, res) => {
    res.send('BackChat API is running. Use /graphql endpoint for queries.');
  });
  
  app.use((err, req, res, next) => {
    console.error('Express error:', err);
    res.status(500).send('Something went wrong');
  });
  
  const PORT = 4000;
  httpServer.listen(PORT, () => {
    console.log(`Server is running at http://localhost:${PORT}/graphql`);
    console.log(`WebSocket server is ready at ws://localhost:${PORT}/graphql`);
  });
}

startServer().catch(err => {
  console.error('Error starting the server:', err);
});