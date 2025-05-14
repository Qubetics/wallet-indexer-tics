import { config } from "../config";
import kafkaHelper from "../config/kafka.helper";
import { Wallet_Helper } from "../helpers";
import { WalletModel } from "../models";

class UpdateBalanceController {
    public tics_pending_balance_update_topic = config.config.KAFKA_BALANCE.TICS_PENDING_BALANCE_UPDATE_TOPIC;

    public update_wallet_balance = async () => {
        let consumer = await kafkaHelper.kafka.consumer({ groupId: config.config.KAFKA_BALANCE.TICS_PENDING_BALANCE_UPDATE_GROUP_ID });
        await consumer.subscribe({ topic: this.tics_pending_balance_update_topic, fromBeginning: true });
        await consumer.run({
            eachMessage: async ({ topic, partition, message }: { topic: any, partition: any, message: any }) => {
                console.log("🚀 ~ startTxStatusUpdateQueue topic >>>>", topic);
                console.log("🚀 ~ startTxStatusUpdateQueue message.value >>>>", message.value.toString());
                await this.updateBalance(JSON.parse(message.value.toString()))
            }
        }).catch((err: Error) => {
            console.log("🔥 ~ ~ startTxStatusUpdateQueue error", err);
        });
    };

    public updateBalance = async (data: any) => {
        try {
            if (data.queue_count < 10) {

                let balance: any = await Wallet_Helper.Update_Balance_Queue(data.wallet_address, data.coin_data);
                if (balance !== null) {
                    await WalletModel.update(
                        { balance: Number(balance) },
                        { where: { wallet_address: data.wallet_address, coin_id: data.coin_data.coin_id } })
                } else {
                    if (data.queue_count <= 5) {
                        await kafkaHelper.produceMessage(this.tics_pending_balance_update_topic, {
                            coin_data: data.coin_data,
                            wallet_address: data.wallet_address,
                            queue_count: data.queue_count + 1
                        });
                    }
                }

                console.log("balance:::", balance);
            }
            else {
                console.log(`updating balance attempts are more than 10 `);
            }

        } catch (err: any) {
            console.error("Error in updateBalance", err)    
        }
    }



}


export default new UpdateBalanceController();