import { config } from "../config/config";
import { BlockChainStatusEnum, BooleanEnum, NotificationTypeEnum, TokenStandard, TrnxTypeEnum } from "../enum";
import { NotificationInterface, TrnxHistoryModel } from "../models";
import { Utility_Helper, Wallet_Helper, Blockchain_Helper } from "../helpers";
import ticsProcessHelper from "./process.helper";
import { Op } from "sequelize";
import { global_helper } from "../helpers/global_helper";
import kafkaHelper from "../config/kafka.helper";
import { ethers, TransactionReceipt, TransactionResponse } from "ethers";

class TicsTxStatusUpdateProcess {

  public startTxStatusUpdateQueue = async () => {
    let consumer = await kafkaHelper.kafka.consumer({ groupId: config.KAFKA_TRANSACTIONS.TICS_PENDING_WITHDRAWAL_TX_GROUP_ID });
    await consumer.subscribe({ topic: config.KAFKA_TRANSACTIONS.TICS_PENDING_WITHDRAWAL_TX_TOPIC, fromBeginning: true });
    await consumer.run({
      eachMessage: async ({ topic, partition, message }: { topic: any, partition: any, message: any }) => {
        console.log("🚀 ~ startTxStatusUpdateQueue topic >>>>", topic);
        console.log("🚀 ~ startTxStatusUpdateQueue message.value >>>>", message.value.toString());
        await this.getTx(JSON.parse(message.value.toString()))
      }
    }).catch((err: Error) => {
      console.log("🔥 ~ ~ startTxStatusUpdateQueue error", err);
    });
  };

  public getTx = async (data: { tx_id: string; coin_id: number }) => {
    console.log('Pending data >>>>>>>', data);
    try {
      // Initialize provider
      const provider = Blockchain_Helper.provider;

      const transaction: TransactionResponse | null = await provider.getTransaction(data.tx_id);

      if (transaction && transaction.blockNumber) {
        const transactionReceipt: TransactionReceipt | null = await provider.getTransactionReceipt(data.tx_id);

        let gasReverted: number | undefined;
        const gasUsed = transactionReceipt?.gasUsed || BigInt(0);
        const gasPrice = transaction.gasPrice || BigInt(0);
        const gasTotal = transaction.gasLimit || BigInt(0);
        const txFee = await Utility_Helper.bigNumberSafeMath(
          gasUsed, '*', gasPrice
        )
        // Calculate gas in ETH (using ethers v6 formatting utils)
        const gasInTics: number = Number(
          ethers.formatEther(txFee)
        );

        if (gasTotal > BigInt(0) && gasUsed > BigInt(0)) {
          const gasDiff = await Utility_Helper.bigNumberSafeMath(gasTotal, '-', gasUsed);
          const reverted = await Utility_Helper.bigNumberSafeMath(gasPrice, '*', gasDiff);

          gasReverted = Number(ethers.formatEther(reverted));
        }

        if (transactionReceipt && transactionReceipt?.status && transaction?.blockHash != null) {
          const txUpdate = {
            coin_id: data.coin_id,
            status: "success",
            txid: transaction.hash,
            gasReverted: gasReverted !== undefined ? gasReverted : undefined,
            fromAddress: transactionReceipt.from.toLowerCase(),
            tx_fee: gasInTics
          };
          await this.sendTxUpdate(txUpdate);
        } else if (transactionReceipt && transactionReceipt?.status === 0) {
          const txUpdate = {
            coin_id: data.coin_id,
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

  public sendTxUpdate = async (txData: {
    coin_id: number;
    status: string;
    txid: string;
    gasReverted: number | undefined;
    fromAddress: string;
    tx_fee: number
  }) => {
    try {
      let coin_data: any = await ticsProcessHelper.CoinByCoinId(txData.coin_id);
      const trx_data = {
        coin_id: txData?.coin_id,
        status: txData?.status,
        txid: txData?.txid,
        gas_reverted: txData?.gasReverted,
        tx_fee: txData?.tx_fee,
        fromAddress: txData?.fromAddress,
        coin: {
          coin_id: txData?.coin_id,
          is_token: coin_data.is_token,
          token_type: coin_data.token_type,
          coin_symbol: coin_data.coin_symbol,
          coin_family: coin_data.coin_family,
          cmc_id: coin_data.cmc_id,
          token_address: Number(coin_data.is_token) === Number(BooleanEnum.true) ? coin_data.token_address : coin_data.coin_symbol,
          added_by: coin_data.added_by,
        }
      };
      console.log(trx_data, "withdrwal");
      await this.updateBroadcastTx(trx_data);


    } catch (error: any) {
      console.error('sendTxUpdate error >>>>>>>', error);
      return false;
    }
  };

  public async updateBroadcastTx(
    trx_data: any
  ) {
    const txnData: any = await TrnxHistoryModel.findOne({ attributes: ['from_adrs', 'amount', 'user_id', 'id', 'to_adrs', 'type', 'coin_family', 'fiat_type'], where: { tx_id: trx_data.txid, status: 'completed', [Op.or]: [{ blockchain_status: { [Op.is]: null as any } }, { blockchain_status: 'pending' }] }, raw: true });
    if (txnData) {
      let from_address = txnData.from_adrs;
      let amount = txnData.amount;
      let user_id = txnData.user_id;
      let tx_row_id = txnData.id;
      let status_tx = trx_data.status == "success" ? BlockChainStatusEnum.CONFIRMED : BlockChainStatusEnum.FAILED;
      let to_address = txnData.to_adrs;
      let tx_type = txnData?.type
      let coin_symbol = trx_data?.coin?.coin_symbol
      let coin_family = txnData.coin_family;

      //Update transaction status
      let txUpdateRes = await TrnxHistoryModel.update({ blockchain_status: status_tx, tx_fee: trx_data.tx_fee, gas_reverted: trx_data.gas_reverted }, { where: { id: tx_row_id } })
      if (txUpdateRes) {
        if (status_tx == BlockChainStatusEnum.CONFIRMED) {
          /** save rewrad */
          let AddressWithdraw: any = await ticsProcessHelper.check_our_wallet_address(from_address);
          if (AddressWithdraw) {
            /** update withdraw user tics balance */
            if (tx_type !== TrnxTypeEnum.DAPP) {
              const native_coin: any = await ticsProcessHelper.NativeCoinByCoinFamily(coin_family);
              await Wallet_Helper.Update_Balance(from_address, native_coin);
              if (trx_data?.coin?.is_token === BooleanEnum.true && trx_data.coin?.token_type.toLowerCase() === TokenStandard.ERC20.toLowerCase()) {
                /** Update User ERC20 Token Balance */
                await Wallet_Helper.Update_Balance(from_address, trx_data?.coin);
              }
            } else { // Entered in DAPP transactioin
              await Wallet_Helper.Update_all_active_coin_balance(from_address, coin_family);
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
                trnxTypeW = "Cross-chain Swap";
                break;
              default:
                break;
            }
            const notiMsg = `${trnxTypeW} of ${await Utility_Helper.bigNumberSafeConversion(
              amount
            )} ${coin_symbol.toUpperCase()} has been confirmed.`;

            let notifData: NotificationInterface = {
              title: NotificationTypeEnum.WITHDRAW.toUpperCase(),
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
            Utility_Helper.SendNotification(notifData);
          }
          let Addressdeposit: any = await ticsProcessHelper.check_our_wallet_address(to_address);
          if (Addressdeposit) {
            let to_user_id: number = Addressdeposit ? Addressdeposit.user_id : 0;
            let trnxTypeD: string = "Deposit";

            /** update to adrs wallet balance */
            if (to_address !== from_address) {
              if (tx_type !== TrnxTypeEnum.DAPP) {
                let balance: any = await Wallet_Helper.Update_Balance(
                  to_address, /// wallet_address,
                  trx_data?.coin /// coin
                );
                await global_helper.check_coin_in_wallet(to_address, trx_data?.coin, balance)
              } else {
                await Wallet_Helper.Update_all_active_coin_balance(to_address, coin_family);
              }
            }
            const notiMsg = `${trnxTypeD} of ${await Utility_Helper.bigNumberSafeConversion(
              amount
            )} ${trx_data?.coin?.coin_symbol.toUpperCase()} has been confirmed.`;

            let notifData: NotificationInterface = {
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
              tx_type: tx_type,
              notification_type: trnxTypeD,
              state: "confirmed"
            };
            Utility_Helper.SendNotification(notifData);
          }

          return true;

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
              trnxTypeW = "Cross-chain Swap";
              break;
            default:
              break;
          }

          let notiMsg = trx_data?.coin.token_type === "ERC721" ? `Withdraw of NFT(${trx_data?.coin.coin_symbol.toUpperCase()}) with token id ${amount} has been confirmed.` : `${trnxTypeW} request of ${await Utility_Helper.bigNumberSafeConversion(amount)} ${trx_data?.coin.coin_symbol.toUpperCase()} has been failed.`;
          let notifData: NotificationInterface = {
            title: NotificationTypeEnum.WITHDRAW.toUpperCase(),
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
            notification_type: NotificationTypeEnum.WITHDRAW,
            state: "failed"
          };
          Utility_Helper.SendNotification(notifData);

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
