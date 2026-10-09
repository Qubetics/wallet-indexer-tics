import { config } from "../config/config";
import { Op } from "sequelize";
import { BlockChainStatusEnum } from "../enum";
import { CoinsModel, TrnxHistoryModel } from "../models";
import kafkaHelper from "../config/kafka.helper";
import { pending_trnx } from "../interfaces/global_helper";

class TICSPendingWithdrawalProcess {
  public tics_pending_withdrawal_tx_topic = config.KAFKA_TRANSACTIONS.TICS_PENDING_WITHDRAWAL_TX_TOPIC;

  public getTransactionFromDB = async () => {
    let tx: any = await TrnxHistoryModel.findAll({
      attributes: ["tx_id", "coin_id"],
      include: [{
        model: CoinsModel,
        attributes: ['is_token', 'token_type', 'token_address'],
        as: "coin_data"
      }],
      where: {
        status: 'completed',
        [Op.or]: [{ blockchain_status: null }, { blockchain_status: BlockChainStatusEnum.PENDING }],
        req_type: { [Op.notIn]: ['TRANSAK', 'ALCHEMY'] },
        coin_family: config.STATIC_COIN_FAMILY.TICS
      },
    })
    if (tx.length > 0) {
      await Promise.all(tx.map(async (el: any) => {
        const trnx: pending_trnx = {
          tx_id: el.tx_id,
          coin_id: el.coin_id,
          is_token: el.coin_data.is_token,
          token_type: el.coin_data.token_type,
          token_address: el.coin_data.token_address,
        };
        return this.addTxToQueue(trnx); // Directly return the promise
      }));
    }
  };

  public addTxToQueue = async (data: { tx_id: string; coin_id: number }) => {
    await kafkaHelper.produceMessage(this.tics_pending_withdrawal_tx_topic, data);
  };
}

export const ticsPendingWithdrawalProcess = new TICSPendingWithdrawalProcess();
