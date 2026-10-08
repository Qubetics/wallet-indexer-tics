import zookeeper from 'node-zookeeper-client';
import { config } from 'dotenv';
config();
const client = zookeeper.createClient(
    process.env.ZOOKEEPER_HOST ||
    'localhost:2181'
);
class ZookeeprProvider {
    config: any = {};
    constructor() {
        client.connect();
    }

    connectZookeeper = async () => {

        return new Promise((resolve, reject) => {
            console.debug(`connectZookeeper...`, process.env.ZOOKEEPER_HOST);
            console.debug(`process.env.ZOOKEEPER_PATH...`, process.env.ZOOKEEPER_PATH);
            client.once('connected', () => {

                client.getData(
                    process.env.ZOOKEEPER_PATH || '/qubetics_wallet',
                    (err: any, data: any, _: any) => {
                        if (err) {
                            console.error(`connectZookeeper err...`, err);
                            reject(err);
                        }
                        if (data) {
                            this.config = JSON.parse(data.toString('utf8'));
                            console.log(`Zookeeper connected...`);
                        }
                        resolve(true);
                    }
                );
            });
        });
    }
}

const zookeeperConfig = new ZookeeprProvider();

export default zookeeperConfig;