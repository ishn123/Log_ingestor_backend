class LogIngestor {

    constructor(kafkaUrl) {
        this.kafkaUrl = kafkaUrl;
        this.headers = {
            'Content-Type': 'application/vnd.kafka.json.v2+json',
            'Accept': 'application/vnd.kafka.v2+json',
        };
    }


    async publishDataToKafka(logData) {
        try {
            const response = await fetch(this.kafkaUrl, {
                method: "POST",
                headers: this.headers,
                body: JSON.stringify(logData)
            })

            if (response.status === 200) {
                console.log(`Data published to Kafka successfully. Status code: ${response.status}`);
                return true;
            } else {
                console.error(`Failed to publish data to Kafka. Status code: ${response.status}`);
                return false;
            }
        } catch (e) {
            console.error(`Error publishing data to Kafka: ${e.message}`);
            return false;
        }
    }
}



module.exports = LogIngestor;