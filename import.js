const config = require('config');
const csv = require('csv-parser');
const fs = require('fs');
const {Client} = require('@elastic/elasticsearch');
const indexName = config.get('elasticsearch.index_name');

async function run() {
    // Create Elasticsearch client
    const client = new Client({node: config.get('elasticsearch.uri')});

    // Création de l'indice
    client.indices.create({index: indexName}, (err, resp) => {
        if (err) console.trace(err.message);
    });

    let anomalies = [];
    // Read CSV file
    fs.createReadStream('dataset/dans-ma-rue.csv')
        .pipe(csv({
            separator: ';'
        }))
        .on('data', (data) => {
            anomalies.push({
                '@timestamp': data["DATEDECL"],
                object_id: data["OBJECTID"],
                annee_declaration: data["ANNEE DECLARATION"],
                mois_declaration: data["MOIS DECLARATION"],
                type: data["TYPE"],
                sous_type: data["SOUSTYPE"],
                code_postal: data["CODE_POSTAL"],
                ville: data["VILLE"],
                arrondissement: data["ARRONDISSEMENT"],
                prefixe: data["PREFIXE"],
                intervenant: data["INTERVENANT"],
                conseil_de_quartier: data["CONSEIL DE QUARTIER"],
                location: data["geo_point_2d"]
            });
            // console.log(anomalies);
        })
        .on('end', async () => {
            let buffer = [];
            for (let i = 0; i < anomalies.length; i++) {
                buffer.push(anomalies[i]);
                if ((i % 20000 === 0 && i !== 0) || i === anomalies.length - 1) {

                    console.log(i);
                    client.bulk(createBulkInsertQuery(buffer), (err, resp) => {
                        if (err) console.trace(err.message);
                        else console.log(`Inserted ${resp.body.items.length} anomalies`);
                        client.close();
                    });
                    await sleep(1000);
                    buffer = [];
                }
            }

            console.log('Terminated!');
        });
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
function createBulkInsertQuery(anomalies) {
    const body = anomalies.reduce((acc, anomalie) => {
        const {object_id, ...rest} = anomalie;
        acc.push({index: {_index: indexName, _type: '_doc', _id: anomalie.object_id}})
        acc.push(rest);
        return acc
    }, []);

    return {body};
}

run().catch(console.error);
