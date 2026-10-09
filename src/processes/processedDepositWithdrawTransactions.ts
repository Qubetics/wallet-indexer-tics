import { TxType, WebHookTxType } from "../types/ERC20Tokens.types";
import { CoinInterface, NotificationInterface, trnxHistoryModel } from "../models";
import { config } from "../config/config";
import ticsProcessHelper from "./process.helper";
import { BlockChainStatusEnum, TokenStandard, TrnxTypeEnum, TxReqTypesEnum } from "../enum";
import { utilityHelper, walletHelper } from "../helpers"
import { Op } from "sequelize";
import { globalHelper } from "../helpers/global.helper";
import WalletModel from "../models/tables/model.wallets";
import kafkaHelper from "../config/kafka.helper";


class TicsDepositWithdrawProcess {

  public startDepositQueue = async () => {
    let consumer: any = null;
    let isConnected = false;

    const connectConsumer = async () => {
      try {

        if (consumer) {
          await consumer.disconnect();
        }

        consumer = await kafkaHelper.kafka.consumer({
          groupId: config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_GROUP_ID,
          // Add session timeout for better error handling
          sessionTimeout: 30000,
          // Add heartbeat interval
          heartbeatInterval: 3000,
        });

        await consumer.subscribe({
          topic: config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_TOPIC,
          fromBeginning: true, // ✅ Changed to false to prevent reprocessing
        });

        await consumer.run({
          // ✅ Add proper autoCommit configuration
          autoCommit: true,
          autoCommitInterval: 5000,
          autoCommitThreshold: 100,

          eachMessage: async ({ topic, partition, message }: { topic: any; partition: any; message: any }) => {
            try {
              console.log("🚀 ~ startDepositQueue topic >>>>", topic);
              console.log("🚀 ~ startDepositQueue message.value >>>>", message.value.toString());
              await this.checkIfOurTx(JSON.parse(message.value.toString()))

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
            console.log(`🔄 Attempting to connect consumer startDepositQueue...`);
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

  public checkIfOurTx = async (data: TxType) => {
    if (data.amount == 0) {
      console.log("amount === 0")
    } else {
      let coin_data: any = null;
      if (data.token) {
        coin_data = data.token;
      } else if (data.isNFTToken) {
        coin_data = data.isNFTToken;
      } else {
        coin_data = await ticsProcessHelper.checkIfContract('NativeCoin');
      }
      const webhookTx: WebHookTxType = {
        coin: coin_data,
        tx_id: data.txId,
        to_address: data.toAddress,
        from_address: data.fromAddress,
        block_id: data.blockId,
        amount: data.amount,
        isNFTToken: data.isNFTToken || false,
        status: 'completed',
        blockchain_status: BlockChainStatusEnum.CONFIRMED,
        txType: data.txType,
        trnx_fee: data.trnx_fee
      };
      await this.SaveNewTrnx(webhookTx);
    }
  };

  SaveNewTrnx = async (
    trnx_data: WebHookTxType
  ) => {
    try {
      const tx_hash: string = trnx_data.tx_id;
      const to_address: string = trnx_data.to_address;
      const from_address: string = trnx_data.from_address;
      const coin: CoinInterface = trnx_data.coin;
      const amount: number = trnx_data.amount;
      const status: string = trnx_data.status;
      const block_number: number = trnx_data.block_id;
      const blockchain_status: BlockChainStatusEnum = trnx_data.blockchain_status;
      const trnx_fee: any = trnx_data.trnx_fee ? trnx_data.trnx_fee : 0;
      let tx_type: TrnxTypeEnum = trnx_data.txType ? trnx_data.txType : TrnxTypeEnum.DEPOSIT;
      const fiat_currency = "USD";
      let checkOldTrnx: any = null;
      if (tx_type == TrnxTypeEnum.WITHDRAW) {
        checkOldTrnx = await trnxHistoryModel.findOne({
          attributes: ["coin_id", "tx_id", "type"],
          where: {
            tx_id: {
              [Op.like]: tx_hash,
            },
            from_adrs: {
              [Op.like]: from_address,
            },
            coin_id: coin.coin_id,
          },
          raw: true,
        });
        if (checkOldTrnx == null) {
          const checkCoinInWallet: any = await WalletModel.findOne({
            attributes: ["coin_id", "coin_family"],
            where: {
              wallet_address: from_address,
              coin_id: coin.coin_id
            },
            raw: true
          })
          if (!checkCoinInWallet) {
            console.error(`Coin not exist under this wallet.`);
            return false;
          }
        }
      } else if (tx_type == TrnxTypeEnum.DEPOSIT) {
        checkOldTrnx = await trnxHistoryModel.findOne({
          attributes: ["coin_id", "tx_id", "type"],
          where: {
            tx_id: {
              [Op.like]: tx_hash,
            },
            to_adrs: {
              [Op.like]: to_address,
            },
            coin_id: coin.coin_id,
          },
          raw: true,
          logging: true
        });
      }

      if (checkOldTrnx) {
        console.error(`Transaction ${tx_hash} is already exists.`);
        return false;
      }

      if (tx_type !== TrnxTypeEnum.WITHDRAW && checkOldTrnx && checkOldTrnx.tx_id == tx_hash) {
        tx_type = checkOldTrnx.type;
      }

      const addresses = [from_address, to_address].filter(Boolean);
      const results = await ticsProcessHelper.getMultipleKeyValuePairs(`${config.TICS_WALLET_ADDRESS}`, addresses);
      console.log('results 2222222222 >>>>', results);
      if (results) {
        const [addressWithdraw, addressDeposit]: any = results as string[];
        const user_id: number = addressWithdraw ? JSON.parse(addressWithdraw)?.user_id : 0;
        const to_user_id: number = addressDeposit ? JSON.parse(addressDeposit)?.user_id : 0;

        const trx_data: any = {
          user_id: user_id,
          to_user_id: to_user_id,
          req_type: TxReqTypesEnum.EXNG,
          from_adrs: from_address,
          to_adrs: to_address,
          coin_id: coin.coin_id,
          coin_family: coin.coin_family,
          amount: amount,
          status: status,
          block_id: block_number,
          tx_id: tx_hash,
          blockchain_status: blockchain_status,
          type: tx_type,
          tx_fee: trnx_fee,
          fiat_type: fiat_currency
        }
        const newTrnx: any = await trnxHistoryModel.create(trx_data);

        if (addressDeposit) {
          const trnxTypeD: string = "Deposit";

          if (coin?.token_type === null || coin?.token_type?.toLowerCase() === TokenStandard.ERC20.toLowerCase()) {
            /** update to adrs wallet balance */
            const balance: any = await walletHelper.updateBalance(
              to_address, /// wallet_address,
              coin /// coin
            );
            await globalHelper.checkCoinInWallet(to_address, coin, balance)
          }

          const notiMsg = `${trnxTypeD} of ${await utilityHelper.bigNumberSafeConversion(
            amount
          )} ${coin?.coin_symbol?.toUpperCase()} has been confirmed.`;

          const notifData: NotificationInterface = {
            title: trnxTypeD?.toUpperCase(),
            message: notiMsg,
            amount: amount,
            from_user_id: user_id,
            to_user_id: to_user_id,
            wallet_address: to_address,
            tx_id: newTrnx.id,
            coin_symbol: coin.coin_symbol,
            coin_id: coin.coin_id,
            coin_family: coin.coin_family,
            tx_type: tx_type,
            notification_type: trnxTypeD,
            state: "confirmed"
          };
          utilityHelper.sendNotification(notifData);
        }

        if (addressWithdraw) {
          let trnxTypeW: string = "Withdraw";

          switch (tx_type) {
            case TrnxTypeEnum.SMARTCONTRACTINTERACTION:
              trnxTypeW = "Smart Contract Interaction";
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
          if (coin?.token_type === null || coin?.token_type?.toLowerCase() === TokenStandard.ERC20.toLowerCase()) {
            await walletHelper.updateBalance(
              from_address, /// wallet_address,
              coin /// coin
            );
          }
          if (coin.is_token) {
            const native_coin: any =
              await ticsProcessHelper.checkIfContract('NativeCoin');
            await walletHelper.updateBalance(
              from_address, /// wallet_address,
              native_coin /// coin
            );
          }

          const notiMsg = `${trnxTypeW} request of ${await utilityHelper.bigNumberSafeConversion(amount)} ${coin.coin_symbol?.toUpperCase()} has been confirmed.`;

          const notifData: NotificationInterface = {
            title: trnxTypeW?.toUpperCase(),
            amount: amount,
            message: notiMsg,
            from_user_id: 0,
            to_user_id: user_id,
            wallet_address: from_address,
            tx_id: newTrnx.id,
            tx_type: tx_type,
            coin_symbol: coin.coin_symbol,
            coin_family: coin.coin_family,
            coin_id: coin.coin_id,
            notification_type: trnxTypeW,
            state: "confirmed"
          };

          utilityHelper.sendNotification(notifData);
        }

        return true;
      } else {
        console.log('Not own address');
      }
    } catch (error: any) {
      console.error(`SaveTrnx_Process-SaveNewTrnx error >>>`, error);
      return false;
    }
  };

}
export const ticsWithdrawDepositProcess = new TicsDepositWithdrawProcess();
