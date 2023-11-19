const {workerData} = require("worker_threads");
const KafkaLogConsumer = require("./consumer");

async function startLogConsumerThread() {
    const consumer = new KafkaLogConsumer(...workerData);

    try {
        await consumer.consumeAndStoreLogs();
    } catch (error) {
        console.error('Error in consumeAndStoreLogs:', error);
    } finally {
        
    }
}

startLogConsumerThread();