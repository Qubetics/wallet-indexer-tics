import { config } from "../config";
import kafkaHelper from "../config/kafka.helper";
import { CoinFamilyEnum } from "../enum";
import { Wallet_Helper } from "../helpers";
import { CoinsModel, WalletModel } from "../models";
import ticsProcessHelper from "./process.helper";


class UpdateBalanceController {
    public tics_pending_balance_update_topic = config.config.KAFKA_BALANCE.TICS_PENDING_BALANCE_UPDATE_TOPIC;

    public update_wallet_balance = async () => {
        let consumer = await kafkaHelper.kafka.consumer({ groupId: config.config.KAFKA_BALANCE.TICS_PENDING_BALANCE_UPDATE_GROUP_ID });
        await consumer.subscribe({ topic: this.tics_pending_balance_update_topic, fromBeginning: true });
        await consumer.run({
            eachMessage: async ({ topic, partition, message }: { topic: any, partition: any, message: any }) => {
                //console.log("🚀 ~ startTxStatusUpdateQueue topic >>>>", topic);
                //console.log("🚀 ~ startTxStatusUpdateQueue message.value >>>>", message.value.toString());
                await this.updateBalance(JSON.parse(message.value.toString()))
            }
        }).catch((err: Error) => {
            console.log("🔥 ~ ~ startTxStatusUpdateQueue error", err);
        });
    };

    public updateBalance = async (data: any) => {
        try {
            if (data.queue_count > 5) {
                console.log(`Balance update attempts exceeded (5) for address: ${data.wallet_address}`);
                return;
            }

            const balance: any = await Wallet_Helper.fetchBalanceForAddress(data.wallet_address, data.coin_data);
            if (balance !== null) {
                const currentBalance = data.balance !== undefined ? Number(data.balance) : undefined;
                if (currentBalance === undefined || currentBalance !== balance) {
                    console.log(`Balance updated for ${data.wallet_address}: ${balance}`);
                    await WalletModel.update(
                        { balance: Number(balance) },
                        { where: { wallet_address: data.wallet_address, coin_id: data.coin_data.coin_id } });
                }
            } else {
                if (data.queue_count <= 5) {
                    const queueData: any = {
                        coin_data: data.coin_data,
                        wallet_address: data.wallet_address,
                        queue_count: data.queue_count + 1,
                    }
                    if (data.balance !== undefined) {
                        queueData.balance = data.balance;
                    }
                    await kafkaHelper.produceMessage(this.tics_pending_balance_update_topic, queueData);
                }
            }


        } catch (err: any) {
            console.error("Error in updateBalance", err)
        }
    }

    public updateWalletBalanceByWallet = async () => {
        try {
            let coinLimitCount = 0;

            const pageCounter = `WalletCount_${CoinFamilyEnum.TICS}`;
            const limit = 20;

            const findCoinCounter = await ticsProcessHelper.getKeyValuePair(config.config.REDISKEYS.ALL_PROCESSES, pageCounter)
            //console.log("🚀 ~ ticsUpdateBalanceController ~ publicupdateWalletBalanceByWal ~ findCoinCounter:", findCoinCounter)

            if (findCoinCounter) {
                coinLimitCount = Number(findCoinCounter);
            }
            //console.log("🚀 ~ ticsUpdateBalanceController ~ publicupdateWalletBalanceByWal ~ coin_limit_count:", coinLimitCount)
            const wallets: any = await WalletModel.findAll({
                attributes: ['wallet_address', 'balance'],
                where: {
                    coin_family: CoinFamilyEnum.TICS
                },
                include: [
                    {
                        attributes: ["coin_id", "coin_name", "coin_symbol", "coin_family", "is_token", "token_type", "token_address"],
                        model: CoinsModel,
                        as: 'coin_data',
                        where: { is_token: 0 }
                    }
                ],
                limit,
                offset: coinLimitCount,
                order: [['wallet_id', 'ASC']],
                nest: true,
                // logging: true
            });
            if (!wallets) {
                await ticsProcessHelper.setKeyValuePair(config.config.REDISKEYS.ALL_PROCESSES, pageCounter, '0')
            }
            if (wallets.length > 0) {

                for (const wallet of wallets) {
                    const coin_data = wallet.coin_data;
                    if (!coin_data) return false;
                    const payload = {
                        coin_data: coin_data,
                        wallet_address: wallet.wallet_address,
                        balance: wallet.balance,
                        queue_count: 0,
                    };

                    await kafkaHelper.produceMessage(this.tics_pending_balance_update_topic, payload);
                }
            }
            if (wallets.length < limit) {
                // console.log("inside wallets limit")
                await ticsProcessHelper.setKeyValuePair(config.config.REDISKEYS.ALL_PROCESSES, pageCounter, '0')
            } else {
                coinLimitCount += limit;
                // console.log("incrementing cnt")
                await ticsProcessHelper.setKeyValuePair(config.config.REDISKEYS.ALL_PROCESSES, pageCounter, coinLimitCount.toString())
            }
            // console.log("\nEND")
            return true;
        } catch (err: any) {
            console.error("Error in updateWalletBalanceByWallet", err)
        }
    };



}


export default new UpdateBalanceController();