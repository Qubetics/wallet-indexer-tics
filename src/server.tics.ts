import kafkaHelper from "./config/kafka.helper";
import { ticsWithdrawDepositProcess, ticsPendingWithdrawalProcess, ticsTxStatusUpdateProcess, ticsBlockProcess, processedBlockTransactions, updateBalanceProcess } from "./processes/index.process"
import schedule from "node-schedule";
class TICSStartProcess {
  constructor() {
    this.connectKafka();
    this.readBlockProcess();
    this.readSpecificBlock();
    this.consumeBlockTxQueue();
    this.consumeBehindBlockTxQueue();
    this.consumeWithdrawDepositTxsQueue();
    this.internalTxsInQueue();
    this.consumeInternalTxQueue();
    this.consumeUpdateBalanceQueue();
  }

  public async connectKafka() {
    await kafkaHelper.connect();
  }

  public readBlockProcess() {
    schedule.scheduleJob("*/8 * * * * *", async function () {
      await ticsBlockProcess.readCurrentBlocks();
      await ticsBlockProcess.readBehindBlocks();
    });
  }
  public readSpecificBlock() {
    schedule.scheduleJob('*/1 * * * *', async function () {
      await ticsBlockProcess.readSpecificBlock();
    })
  }
  public consumeBlockTxQueue() {
    setTimeout(async () => {
      await processedBlockTransactions.startBlockTxQueue();

    }, 10000);
  }
  public consumeBehindBlockTxQueue() {
    setTimeout(async () => {
      await processedBlockTransactions.startBehindBlockTxQueue();
    }, 10000);
  }
  public async consumeWithdrawDepositTxsQueue() {
    setTimeout(async () => {
      await ticsWithdrawDepositProcess.startDepositQueue();
    }, 10000);
  }
  public internalTxsInQueue() {
    schedule.scheduleJob("*/10 * * * * *", async function () {
      await ticsPendingWithdrawalProcess.getTransactionFromDB();
    });
  }
  public async consumeInternalTxQueue() {
    setTimeout(async () => {
      await ticsTxStatusUpdateProcess.startTxStatusUpdateQueue();
    }, 10000);
  }
  public async consumeUpdateBalanceQueue() {
    setTimeout(async () => {
      await updateBalanceProcess.updateWalletBalance();
    }, 10000);
  }
}

new TICSStartProcess();
