const config = require('config');
const csv = require('csv-parser');
const fs = require('fs');
const {Client} = require('@elastic/elasticsearch');
const indexName = config.get('elasticsearch.index_name');
const {bufferCount, concatMap, delay} = require('rxjs/operators');
const {from, of} = require('rxjs');

async function run() {
    // Create Elasticsearch client
    const client = new Client({node: config.get('elasticsearch.uri')});

    // Création de l'indice
    client.indices.create({index: indexName}, (err, resp) => {
        if (err) console.trace(err.message);
    });

    let anomalies = [];
    let counter = 0;
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
        .on('end', () => {
            from(anomalies).pipe(
                bufferCount(20000),
                concatMap(data => of(data).pipe(delay(500)))
            ).subscribe(bufferedAnomalies => {
                counter += bufferedAnomalies.length;
                console.log(counter);
                client.bulk(createBulkInsertQuery(bufferedAnomalies), (err, resp) => {
                    if (err) console.trace(err.message);
                    else console.log(`Inserted ${resp.body.items.length} anomalies`);
                    client.close();
                });
            });
        });
}

// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
function createBulkInsertQuery(anomalies) {
    const body = anomalies.reduce((acc, anomalie) => {
        const {object_id, ...rest} = anomalie;
        // Ajout d'un agregat de données pour le top 10 des mois avec le plus d'anomalies.
        const restWithMoisAnnee = {
            ...rest,
            "mois_annee": formatMoisAnnee(rest.mois_declaration, rest.annee_declaration)
        };
        acc.push({index: {_index: indexName, _type: '_doc', _id: anomalie.object_id}})
        acc.push(restWithMoisAnnee);
        return acc
    }, []);

    return {body};
}

function formatMoisAnnee(mois, annee) {
    const formattedNumber = ("0" + mois).slice(-2);
    return `${formattedNumber}/${annee}`;
}

run().catch(console.error);
