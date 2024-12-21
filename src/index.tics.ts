import ZooKeeper from './helpers/zookeeper.helper';


(async () => {
  await ZooKeeper.connectZookeeper();
  await require('./config/index');
  await require('./server.tics');
})();