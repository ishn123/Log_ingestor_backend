const { Client } = require("elasticsearch");

class ElasticSearch {
    constructor(elasticSearchNode) {
        this.elasticsearchNode = elasticSearchNode;
        this.elasticsearchClient = new Client({ node: this.elasticsearchNode });
        this.createElasticsearchIndex();
    }

    async createElasticsearchIndex() {

        const indexExists = await this.elasticsearchClient.indices.exists({ index: 'logs' });
        if (!indexExists) {
            await this.elasticsearchClient.indices.create({
                index: 'logs',
                body: {
                    mappings: {
                        properties: {
                            level: { type: 'keyword' },
                            message: { type: 'text' },
                            resourceId: { type: 'keyword' },
                            timestamp: { type: 'date' },
                            traceId: { type: 'keyword' },
                            spanId: { type: 'keyword' },
                            commit: { type: 'keyword' },
                            parentResourceId: { type: 'keyword' }
                        }
                    }
                }
            });
        }
    }

    async addDatatoElasticSearch(newLog) {
        await this.elasticsearchClient.index({
            index: 'logs',
            body: newLog,
        });
    }

    async searchLogsByDateRange(startDate, endDate) {
        try {
            const response = await this.elasticsearchClient.search({
                index: 'logs',
                body: {
                    query: {
                        range: {
                            timestamp: {
                                gte: startDate.toISOString(),
                                lte: endDate.toISOString(),
                            },
                        },
                    },
                },
            });

            const hits = response.body.hits.hits;
            console.log('Logs within date range:', hits);
            return true;
        } catch (err) {
            console.error('Error searching logs:', err);
            return false;
        }
    }
}

module.exports = ElasticSearch;