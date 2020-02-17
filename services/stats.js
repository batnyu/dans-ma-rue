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
    client.search({
        index: indexName,
        body: {
            "aggs": {
                "arrondissement": {
                    "terms": {
                        "field": "type.keyword",
                        "size": 5
                    },

                    "aggs": {
                        "sous_type": {
                            "terms": {
                                "field": "sous_type.keyword",
                                "size": 5
                            }
                        }
                    }
                }
            }
        }
    }).then(resp => {
        const res = resp.body.aggregations.arrondissement.buckets.map(({key, doc_count, sous_type}) => ({
            type: key,
            count: doc_count,
            sous_types: sous_type.buckets.map(({key, doc_count}) => ({
                sous_type: key,
                count: doc_count
            }))
        }));
        callback(res)
    })
}

exports.statsByMonth = (client, callback) => {
    // TODO Trouver le top 10 des mois avec le plus d'anomalies
    client.search({
        index: indexName,
        body: {
            "size": 0,
            "aggs": {
                "mois_annee": {
                    "terms": {
                        "field": "mois_annee.keyword",
                        "size": 10
                    },
                }
            }
        }
    }).then(resp => {
        const res = resp.body.aggregations.mois_annee.buckets.map(({key, doc_count}) => ({
            month: key,
            count: doc_count
        }));
        callback(res)
    })
}

exports.statsPropreteByArrondissement = (client, callback) => {
    // TODO Trouver le top 3 des arrondissements avec le plus d'anomalies concernant la propreté
    callback([]);
}
