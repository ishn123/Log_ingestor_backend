const exp = require("express");
const bodyparser = require("body-parser");
const cors = require("cors");
const dotenv = require("dotenv");
const LogIngestor = require("./log_ingestion");
const { Worker } = require("worker_threads");
const { DateTime } = require('luxon');
const ElasticSearch = require("./elastic_search");

const Database = require("./database");
dotenv.config();

// server app creation

const app = exp();
app.use(cors());
app.use(bodyparser.json());



const kafka_url = "http://localhost:8082/topics/log-ingestor-service";
const log_ingestor = new LogIngestor(kafka_url)
const elastic = new ElasticSearch("http://localhost:9200")

const user = 'ishan'
const password = 'ishan'
const host = 'mysql-write'
const port = 3306
const database_name = 'log_ingestor_db'



const kafka_bootstrap_servers = 'localhost:9092'
const consumer_group = 'log_consumer_group'
const kafka_topic = 'log-ingestor-service'
const db = new Database(user, password, host, port, database_name);
db.connect();

function startLogConsumerThread() {
    const workerData = [user, password, host, port, database_name, kafka_bootstrap_servers, consumer_group, kafka_topic,'http://localhost:9200']
    const worker = new Worker('./consumer_worker.js',{
        workerData:workerData
    });

    worker.on('message', (message) => {
        console.log(`Worker thread message: ${message}`);
    });

    worker.on('error', (error) => {
        console.error(`Worker thread error: ${error}`);
    });

    worker.on('exit', (code) => {
        console.log(`Worker thread exited with code ${code}`);
    });

}

app.get('/consumer', (req, res) => {
    startLogConsumerThread()
    res.send('Log consumer started in a separate thread.');
});

app.post('/', async (req, res) => {



    const logData = req.body;

    const kafkaData = {
        records: [
            {
                value: {
                    level: logData.level || 'info',
                    message: logData.message || '',
                    resourceId: logData.resourceId || '',
                    timestamp: logData.timestamp || '',
                    traceId: logData.traceId || '',
                    spanId: logData.spanId || '',
                    commit: logData.commit || '',
                    metadata: {
                        parentResourceId: logData.metadata?.parentResourceId || '',
                    },
                },
            },
        ],
    };


    const status = log_ingestor.publishDataToKafka(kafkaData);
    if (status) {
        return res.status(200).json({ message: "Data Published successfully" }).send();
    } else {

        return res.status(500).json({ message: "Failed to Publish data" }).send();
    }

});


app.post('/search', async (req, res) => {
    const searchField = req.body.searchField;
    const searchQuery = req.body.searchQuery;

    const filterCriteria = {
        [searchField]: searchQuery,
    };



    const filteredLogs = await db.getLogsByFilter(filterCriteria);


    const logs = filteredLogs.map((log) => ({
        id: log[0],
        level: log[1],
        message: log[2],
        resourceId: log[3],
        timestamp: DateTime.fromJSDate(log[4]).toFormat('yyyy-MM-dd HH:mm:ss'),
        traceId: log[5],
        spanId: log[6],
        commit: log[7],
        parentResourceId: log[8],
    }));

    res.json({query:logs }).send();
});

app.post('/searchData',async(req,res)=>{
    const startDate = req.body.startDate;
    const endDate = req.body.endDate;

    const status = await elastic.searchLogsByDateRange(startDate,endDate);
    if(status){
        return res.status(200).send();
    }

    return res.status(501).send();
})



app.listen(process.env.PORT, () => {
    console.log("Server started at port........", process.env.PORT);

})