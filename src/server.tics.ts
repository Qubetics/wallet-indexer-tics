import { ticsWithdrawDepositProcess, ticsPendingWithdrawalProcess, ticsTxStatusUpdateProcess, tics_blocks_process, tics_tx_status_deposit_withdraw, TICS_balance_update } from "./processes/index.process"
import schedule from "node-schedule";
class TICSStartProcess {
  constructor() {
    this.read_block_process()
    this.read_behind_block_process()
    this.readSpecificBlock()
    this.consume_read_block_queue()
    this.ticsWithdrawDepositProcess();
    this.getPendingWithdrawalTxProcess();
    this.updateTxStatusProcess();
    this.TICSUpdateBalance();
    this.TICSUpdateBalanceByWallet()
    // this.testing_node()
  }
  public read_block_process() {
    schedule.scheduleJob("*/5 * * * * *", async function () {
      await tics_blocks_process.getBlocks();
      // await tics_blocks_process.readBehindBlock()
    });
  }
  public read_behind_block_process() {
    schedule.scheduleJob("*/6 * * * * *", async function () {
      await tics_blocks_process.readBehindBlock()
    });
  }
  // public testing_node() {
  //   schedule.scheduleJob("*/2 * * * * *", async function () {
  //     await tics_blocks_process.testingNode();
  //   });
  // }
  public readSpecificBlock() {
    schedule.scheduleJob('*/1 * * * *', async function () {
      await tics_blocks_process.readSpecificBlock();
    })
  }
  public consume_read_block_queue() {
    setTimeout(async () => {
      await tics_tx_status_deposit_withdraw.startBlockTxQueue();
      await tics_tx_status_deposit_withdraw.startBehindBlockTxQueue();
    }, 10000);
  }
  public async ticsWithdrawDepositProcess() {
    setTimeout(async () => {
      await ticsWithdrawDepositProcess.startDepositQueue();
    }, 10000);
  }
  public getPendingWithdrawalTxProcess() {
    schedule.scheduleJob("*/10 * * * * *", async function () {
      await ticsPendingWithdrawalProcess.getTransactionFromDB();
    });
  }
  public async updateTxStatusProcess() {
    setTimeout(async () => {
      await ticsTxStatusUpdateProcess.startTxStatusUpdateQueue();
    }, 10000);
  }
  public async TICSUpdateBalance() {
    setTimeout(async () => {
      await TICS_balance_update.default.update_wallet_balance();
    }, 10000);
  }

  // TICS Update Balance By Wallet
  public async TICSUpdateBalanceByWallet() {
    schedule.scheduleJob("*/1 * * * *", async function () {
      // schedule.scheduleJob("*/50 * * * * *", async function () {
      await TICS_balance_update.default.updateWalletBalanceByWallet();
    });
  }
}

new TICSStartProcess();
