const Database = require("./database");
const { Kafka } = require('kafkajs')
const { Client } = require("elasticsearch");
const ElasticSearch = require("./elastic_search");

class KafkaLogConsumer {
  constructor(user, password, host, port, databaseName, kafkaBootstrapServers, consumerGroup, kafkaTopic,elastic) {
    this.user = user;
    this.password = password;
    this.host = "localhost";
    this.port = port;
    this.databaseName = databaseName;
    this.kafkaBootstrapServers = kafkaBootstrapServers;
    this.consumerGroup = consumerGroup;
    this.kafkaTopic = kafkaTopic;
    this.db = new Database(user, password, host, port, databaseName);
    this.elasticsearchClient = new ElasticSearch(elastic);
    
  }

  

  async consumeAndStoreLogs() {
    const kafka = new Kafka({
      clientId: 'log-consumer',
      brokers: [this.kafkaBootstrapServers]
    });

    const consumer = kafka.consumer({ groupId: this.consumerGroup });

    await consumer.connect();
    await consumer.subscribe({ topic: this.kafkaTopic });

    await consumer.run({
      eachMessage: async ({ message }) => {
        const logData = JSON.parse(message.value.toString());

        // Convert ISO 8601 timestamp to MySQL DateTime format
        const parsedTimestamp = new Date(logData.timestamp);
        const mysqlTimestamp = parsedTimestamp.toISOString().slice(0, 19).replace('T', ' ');

        const newLog = {
          level: logData.level,
          message: logData.message,
          resourceId: logData.resourceId,
          timestamp: mysqlTimestamp,
          traceId: logData.traceId,
          spanId: logData.spanId,
          commit: logData.commit,
          parentResourceId: logData.metadata?.parentResourceId
        };

        try {

          await this.elasticsearchClient.addDatatoElasticSearch(newLog);

          const [rows] = await this.db.insertLog(newLog);
          console.log('Log inserted into MySQL:', rows);
        } catch (err) {
          console.error('Error inserting log into MySQL:', err);
        }
      },
    });
  }

}


module.exports = KafkaLogConsumer;