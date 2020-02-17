const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.statsByArrondissement = (client, callback) => {
    client.search({
        index: indexName,
        body: {
            "size": 0,
            "aggs": {
                "arrondissement": {
                    "terms": {
                        "field": "arrondissement.keyword"
                    },
                }
            }
        }
    }).then(resp => {
        const res = resp.body.aggregations.arrondissement.buckets.map(({key, doc_count}) => ({
            arrondissement: key,
            count: doc_count
        }));
        callback(res)
    })
}

exports.statsByType = (client, callback) => {
    // TODO Trouver le top 5 des types et sous types d'anomalies
    callback([]);
}

exports.statsByMonth = (client, callback) => {
    // TODO Trouver le top 10 des mois avec le plus d'anomalies
    callback([]);
}

exports.statsPropreteByArrondissement = (client, callback) => {
    // TODO Trouver le top 3 des arrondissements avec le plus d'anomalies concernant la propreté
    callback([]);
}
