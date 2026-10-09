import { TxType, WebHookTxType } from "../types/ERC20Tokens.types";
import { CoinInterface, NotificationInterface, TrnxHistoryModel } from "../models";
import { config } from "../config/config";
import ticsProcessHelper from "./process.helper";
import { BlockChainStatusEnum, CoinFamilyEnum, NotificationTypeEnum, TokenStandard, TrnxTypeEnum, TxReqTypesEnum } from "../enum";
import { Utility_Helper, Wallet_Helper } from "../helpers"
import { Op } from "sequelize";
import { global_helper } from "../helpers/global_helper";
import WalletModel from "../models/tables/model.wallets";
import kafkaHelper from "../config/kafka.helper";


class TicsDepositWithdrawProcess {

  public startDepositQueue = async () => {
    let consumer = await kafkaHelper.kafka.consumer({
      groupId: config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_GROUP_ID,
    });
    await consumer.subscribe({
      topic: config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_TOPIC,
      fromBeginning: true,
    });
    await consumer.run({
      eachMessage: async ({
        topic,
        partition,
        message,
      }: {
        topic: any;
        partition: any;
        message: any;
      }) => {
        console.log('🚀 ~ startDepositWithdrawQueue topic', topic);
        console.log('🚀 ~ startDepositWithdrawQueue message.value', message.value.toString());
        await this.checkIfOurTx(JSON.parse(message.value.toString()));
      },
    }).catch((err: Error) => {
      console.log('🔥 ~ startDepositWithdrawQueue consumer error', err);
    });
  };

  public checkIfOurTx = async (data: TxType) => {
    if (data.amount == 0) {
      console.log("amount === 0")
    } else {
      console.log("amount is more than 0", data)
      let coin_data: any = null;
      if (data.token) {
        coin_data = data.token;
      } else if (data.isNFTToken) {
        coin_data = data.token;
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
      console.log(webhookTx, "DEPOSIT");
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
        checkOldTrnx = await TrnxHistoryModel.findOne({
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
          let check_coin_in_wallet: any = await WalletModel.findOne({
            attributes: ["coin_id", "coin_family"],
            where: {
              wallet_address: from_address,
              coin_id: coin.coin_id
            },
            raw: true
          })
          if (!check_coin_in_wallet) {
            console.error(`Coin not exist under this wallet.`);
            return false;
          }
        }
      } else if (tx_type == TrnxTypeEnum.DEPOSIT) {
        checkOldTrnx = await TrnxHistoryModel.findOne({
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
      const AddressWithdraw: any = await ticsProcessHelper.check_our_wallet_address(from_address);
      const Addressdeposit: any = await ticsProcessHelper.check_our_wallet_address(to_address);


      const user_id: number = AddressWithdraw ? AddressWithdraw.user_id : 0;
      const to_user_id: number = Addressdeposit ? Addressdeposit.user_id : 0;

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
      const newTrnx: any = await TrnxHistoryModel.create(trx_data);

      if (Addressdeposit) {
        const trnxTypeD: string = "Deposit";

        if (coin?.token_type === null || coin?.token_type?.toLowerCase() === TokenStandard.ERC20.toLowerCase()) {
          /** update to adrs wallet balance */
          const balance: any = await Wallet_Helper.Update_Balance(
            to_address, /// wallet_address,
            coin /// coin
          );
          await global_helper.check_coin_in_wallet(to_address, coin, balance);
        }

        const isNft = coin?.token_type?.toLowerCase() === TokenStandard.ERC721.toLowerCase() || coin?.token_type?.toLowerCase() === TokenStandard.ERC1155.toLowerCase();


        const notiMsg = isNft ? `Deposit of NFT(${coin?.coin_symbol?.toUpperCase()}) has been confirmed.` : `${trnxTypeD} of ${await Utility_Helper.bigNumberSafeConversion(amount)} ${coin?.coin_symbol?.toUpperCase()} has been confirmed.`;


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
        Utility_Helper.SendNotification(notifData);
      }

      if (AddressWithdraw) {
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
            trnxTypeW = "Cross Chain Swap";
            break;
          default:
            break;
        }

        await Wallet_Helper.Update_Balance(
          from_address, /// wallet_address,
          coin /// coin
        );
        if (coin.is_token) {
          const native_coin: any =
            await ticsProcessHelper.checkIfContract('NativeCoin');
          await Wallet_Helper.Update_Balance(
            from_address, /// wallet_address,
            native_coin /// coin
          );
        }

        const isNft = coin?.token_type?.toLowerCase() === TokenStandard.ERC721.toLowerCase() || coin?.token_type?.toLowerCase() === TokenStandard.ERC1155.toLowerCase();


        const notiMsg = isNft ? `${trnxTypeW} request of NFT(${coin?.coin_symbol?.toUpperCase()}) has been confirmed.` : `${trnxTypeW} request of ${await Utility_Helper.bigNumberSafeConversion(amount)} ${coin?.coin_symbol?.toUpperCase()} has been confirmed.`;


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

        Utility_Helper.SendNotification(notifData);
      }

      return true;
    } catch (error: any) {
      console.error(`SaveTrnx_Process-SaveNewTrnx error >>>`, error);
      return false;
    }
  };

}
export const ticsWithdrawDepositProcess = new TicsDepositWithdrawProcess();
