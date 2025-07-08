import { config } from "../config/config";
import { BlockChainStatusEnum, BooleanEnum, CoinFamilyEnum, NotificationTypeEnum, TokenStandard, TrnxTypeEnum } from "../enum";
import { coinsModel, NotificationInterface, trnxHistoryModel } from "../models";
import { utilityHelper, walletHelper, blockChainHelper, dbHelper } from "../helpers";
import ticsProcessHelper from "./process.helper";
import { Op } from "sequelize";
import { globalHelper } from "../helpers/global.helper";
import kafkaHelper from "../config/kafka.helper";
import { ethers, TransactionReceipt, TransactionResponse } from "ethers";
import { pendingTrnx, TransactionData, TxUpdateData } from "../interfaces/globalHelper";

class TicsTxStatusUpdateProcess {

  public startTxStatusUpdateQueue = async () => {
    let consumer: any = null;
    let isConnected = false;

    const connectConsumer = async () => {
      try {

        if (consumer) {
          await consumer.disconnect();
        }

        consumer = await kafkaHelper.kafka.consumer({
          groupId: config.KAFKA_TRANSACTIONS.TICS_PENDING_WITHDRAWAL_TX_GROUP_ID,
          // Add session timeout for better error handling
          sessionTimeout: 30000,
          // Add heartbeat interval
          heartbeatInterval: 3000,
        });

        await consumer.subscribe({
          topic: config.KAFKA_TRANSACTIONS.TICS_PENDING_WITHDRAWAL_TX_TOPIC,
          fromBeginning: true, // ✅ Changed to false to prevent reprocessing
        });

        await consumer.run({
          // ✅ Add proper autoCommit configuration
          autoCommit: true,
          autoCommitInterval: 5000,
          autoCommitThreshold: 100,

          eachMessage: async ({ topic, partition, message }: { topic: any; partition: any; message: any }) => {
            try {
              console.log("🚀 ~ startTxStatusUpdateQueue topic >>>>", topic);
              console.log("🚀 ~ startTxStatusUpdateQueue message.value >>>>", message.value.toString());
              await this.getTx(JSON.parse(message.value.toString()))

            } catch (error) {
              console.error(`❌ Error processing message:`, error);
            }
          },
        });
        isConnected = true;
      } catch (error) {
        console.error(`❌ Failed to connect consumer:`, error);
        isConnected = false;
        throw error;
      }
    };

    // ✅ Add reconnection logic
    const startWithReconnection = async () => {
      while (true) {
        try {
          if (!isConnected) {
            console.log(`🔄 Attempting to connect consumer startTxStatusUpdateQueue...`);
            await connectConsumer();
          }

          // Keep the consumer alive
          await new Promise(resolve => setTimeout(resolve, 30000));

        } catch (error) {
          console.error(`❌ Consumer connection error:`, error);
          isConnected = false;

          // Wait before retrying
          console.log(`⏳ Waiting 10 seconds before reconnecting...`);
          await new Promise(resolve => setTimeout(resolve, 10000));
        }
      }
    };

    // Start the consumer with reconnection
    await startWithReconnection();
  };

  public getTx = async (data: pendingTrnx) => {
    console.log('Pending data >>>>>>>', data);
    try {
      // Initialize provider
      const provider = blockChainHelper.provider;

      const transaction: TransactionResponse | null = await provider.getTransaction(data.tx_id);
      if (transaction && transaction.blockNumber) {
        const transactionReceipt = await provider.getTransactionReceipt(data.tx_id);

        let gasReverted: number | undefined;
        const gasUsed = transactionReceipt?.gasUsed || BigInt(0);
        const gasPrice = transaction.gasPrice || BigInt(0);
        const gasTotal = transaction.gasLimit || BigInt(0);
        const txFee = await utilityHelper.bigNumberSafeMath(gasUsed, '*', gasPrice);
        // Calculate gas in ETH (using ethers v6 formatting utils)
        const gasInTics: number = Number(
          ethers.formatEther(txFee)
        );

        if (gasTotal > BigInt(0) && gasUsed > BigInt(0)) {
          const gasDiff = await utilityHelper.bigNumberSafeMath(gasTotal, '-', gasUsed);

          const reverted = await utilityHelper.bigNumberSafeMath(gasPrice, '*', gasDiff);
          gasReverted = Number(ethers.formatEther(reverted));
        }

        if (transactionReceipt && transactionReceipt?.status && transaction.blockHash != null) {
          const txUpdate = {
            coin: {
              coin_id: data.coin_id,
              is_token: data.is_token,
              token_type: data.token_type,
              token_address: data.token_address,
            },
            status: "success",
            txid: transaction.hash,
            gasReverted: gasReverted !== undefined ? gasReverted : undefined,
            fromAddress: transactionReceipt.from.toLowerCase(),
            tx_fee: gasInTics
          };
          await this.sendTxUpdate(txUpdate);
        } else if (transactionReceipt && transactionReceipt.status === 0) {
          const txUpdate = {
            coin: {
              coin_id: data.coin_id,
              is_token: data.is_token,
              token_type: data.token_type,
              token_address: data.token_address,
            },
            status: "failed",
            txid: transaction.hash,
            gasReverted: gasReverted !== undefined ? gasReverted : undefined,
            fromAddress: transactionReceipt.from.toLowerCase(),
            tx_fee: gasInTics
          };
          await this.sendTxUpdate(txUpdate);
        }
      }
    } catch (error: any) {
      console.error('getTx >>>>>', error.message);
    }
  };

  public sendTxUpdate = async (txData: TxUpdateData) => {
    try {
      let coin_data: any = {};
      if (!txData.coin?.is_token) {
        console.log('in case of coin');
        coin_data = await ticsProcessHelper.checkIfContract('NativeCoin');
      } else if (txData.coin?.token_type == TokenStandard.ERC20) {
        coin_data = await ticsProcessHelper.checkIfContract(txData.coin.token_address);
      }
      if (coin_data) {
        const trx_data: TransactionData = {
          coin_id: txData.coin?.coin_id,
          status: txData?.status,
          txid: txData?.txid,
          gas_reverted: txData?.gasReverted,
          tx_fee: txData?.tx_fee,
          fromAddress: txData?.fromAddress,
          coin: {
            coin_id: txData.coin.coin_id,
            coin_name: coin_data.coin_name || '',
            coin_symbol: coin_data.coin_symbol,
            coin_family: coin_data.coin_family,
            coin_status: coin_data.coin_status || 0,
            is_token: coin_data.is_token,
            token_type: coin_data.token_type,
            decimals: coin_data.decimals || 0,
            token_address: Number(coin_data.is_token) === Number(BooleanEnum.true) ? coin_data.token_address : coin_data.coin_symbol,
            added_by: coin_data.added_by
          }
        };
        console.log(trx_data, "withdrwal");
        await this.updateBroadcastTx(trx_data);
      } else {
        console.log('not coin present in redis or database');
      }


    } catch (error: any) {
      console.error('sendTxUpdate error >>>>>>>', error);
      return false;
    }
  };

  public async updateBroadcastTx(
    trx_data: TransactionData
  ) {
    const txnData: any = await trnxHistoryModel.findOne({ attributes: ['from_adrs', 'amount', 'user_id', 'id', 'to_adrs', 'type', 'coin_family'], where: { tx_id: trx_data.txid, status: 'completed', [Op.or]: [{ blockchain_status: { [Op.is]: null as any } }, { blockchain_status: 'pending' }] }, raw: true });
    if (txnData) {
      const from_address = txnData.from_adrs;
      const amount = txnData.amount;
      const user_id = txnData.user_id;
      const tx_row_id = txnData.id;
      const status_tx = trx_data.status == "success" ? BlockChainStatusEnum.CONFIRMED : BlockChainStatusEnum.FAILED;
      const to_address = txnData.to_adrs;
      const tx_type = txnData?.type
      const coin_symbol = trx_data?.coin?.coin_symbol
      const coin_family = txnData.coin_family;
      //Update transaction status
      const txUpdateRes = await trnxHistoryModel.update({ blockchain_status: status_tx, tx_fee: trx_data.tx_fee, gas_reverted: trx_data.gas_reverted }, { where: { id: tx_row_id } })
      if (txUpdateRes) {
        if (status_tx == BlockChainStatusEnum.CONFIRMED) {
          /** save rewrad */
          if (tx_type == TrnxTypeEnum.SWAP) {
            console.log("transaction >>", tx_type, "transaction id ", tx_row_id)
          }

          const addresses = [from_address, to_address].filter(Boolean);
          const results = await ticsProcessHelper.getMultipleKeyValuePairs(`${config.TICS_WALLET_ADDRESS}`, addresses);
          if (results) {
            const [addressWithdraw, addressDeposit]: any = results as string[];

            if (addressWithdraw) {
              /** update withdraw user tics balance */
              if (tx_type !== TrnxTypeEnum.DAPP) {
                const native_coin: any = await ticsProcessHelper.checkIfContract('NativeCoin');
                await walletHelper.updateBalance(from_address, native_coin);
                if (trx_data?.coin?.is_token === BooleanEnum.true && trx_data.coin?.token_type?.toLowerCase() === TokenStandard.ERC20.toLowerCase()) {
                  /** Update User ERC20 Token Balance */
                  await walletHelper.updateBalance(from_address, trx_data?.coin);
                }
              } else { // Entered in DAPP transactioin
                await walletHelper.updateAllActiveCoinBalance(from_address, coin_family);
              }

              let trnxTypeW: string = "Withdraw";
              switch (tx_type) {
                case TrnxTypeEnum.DAPP:
                  trnxTypeW = "Smart Contract Execution";
                  break;
                case TrnxTypeEnum.APPROVE:
                  trnxTypeW = "Approval";
                  break;
                case TrnxTypeEnum.SWAP:
                  trnxTypeW = "Swap";
                  break;
                case TrnxTypeEnum.CROSS_CHAIN:
                  trnxTypeW = "Cross chain Swap";
                  break;
                default:
                  break;
              }
              const notiMsg = `${trnxTypeW} request of ${await utilityHelper.bigNumberSafeConversion(amount)} ${coin_symbol.toUpperCase()} has been confirmed.`;

              const notifData: NotificationInterface = {
                title: trnxTypeW.toUpperCase(),
                message: notiMsg,
                amount: amount,
                from_user_id: 0,
                to_user_id: user_id,
                coin_symbol: coin_symbol,
                wallet_address: from_address,
                tx_id: tx_row_id,
                coin_id: trx_data?.coin?.coin_id,
                coin_family: trx_data?.coin.coin_family,
                tx_type: tx_type,
                notification_type: trnxTypeW,
                state: "confirmed"
              };
              utilityHelper.sendNotification(notifData);
            }
            
            if (addressDeposit) {
              const to_user_id: number = addressDeposit ? JSON.parse(addressDeposit)?.user_id : 0;
              /** update to adrs wallet balance */
              if (to_address !== from_address) {
                if (tx_type !== TrnxTypeEnum.DAPP) {
                  if (trx_data.coin?.token_type === null || trx_data.coin?.token_type?.toLowerCase() === TokenStandard.ERC20.toLowerCase()) {
                    const balance: any = await walletHelper.updateBalance(
                      to_address, /// wallet_address,
                      trx_data?.coin /// coin
                    );
                    await globalHelper.checkCoinInWallet(to_address, trx_data?.coin, balance)
                  }
                } else {
                  await walletHelper.updateAllActiveCoinBalance(to_address, coin_family);
                }
              }
              const notiMsg = `Deposit of ${await utilityHelper.bigNumberSafeConversion(amount)} ${trx_data?.coin.coin_symbol} has been confirmed.`;

              const notifData: NotificationInterface = {
                title: NotificationTypeEnum.DEPOSIT.toUpperCase(),
                message: notiMsg,
                amount: amount,
                from_user_id: user_id,
                to_user_id: to_user_id,
                wallet_address: to_address,
                tx_id: tx_row_id,
                coin_symbol: coin_symbol,
                coin_id: trx_data?.coin?.coin_id,
                coin_family: trx_data?.coin?.coin_family,
                tx_type: NotificationTypeEnum.DEPOSIT,
                notification_type: 'Deposit',
                state: "confirmed"
              };
              utilityHelper.sendNotification(notifData);
            }

            return true;
          }

        } else if (status_tx == BlockChainStatusEnum.FAILED) {
          let trnxTypeW: string = "Withdraw";
          switch (tx_type) {
            case TrnxTypeEnum.DAPP:
              trnxTypeW = "Smart Contract Execution";
              break;
            case TrnxTypeEnum.APPROVE:
              trnxTypeW = "Approval";
              break;
            case TrnxTypeEnum.SWAP:
              trnxTypeW = "Swap";
              break;
            case TrnxTypeEnum.CROSS_CHAIN:
              trnxTypeW = "Cross chain Swap";
              break;
            default:
              break;
          }

          const notiMsg = `${trnxTypeW} of ${await utilityHelper.bigNumberSafeConversion(amount)} ${trx_data?.coin.coin_symbol} has been failed.`;

          const notifData: NotificationInterface = {
            title: trnxTypeW.toUpperCase(),
            message: notiMsg,
            amount: amount,
            from_user_id: 0,
            to_user_id: user_id,
            wallet_address: from_address,
            tx_id: tx_row_id,
            coin_symbol: coin_symbol,
            coin_id: trx_data?.coin?.coin_id,
            coin_family: trx_data?.coin?.coin_family,
            tx_type: tx_type,
            notification_type: trnxTypeW,
            state: "failed"
            };
          utilityHelper.sendNotification(notifData);

          return true;
        } else {
          return false;
        }
      } else {
        return false;

      }
    } else {
      return false;

    }
  }
}
export const ticsTxStatusUpdateProcess = new TicsTxStatusUpdateProcess();
