import { config } from "../config";
import kafkaHelper from "../config/kafka.helper";
import { CoinFamilyEnum } from "../enum";
import { CoinInterface, CoinsModel } from "../models";
import WalletModel from "../models/tables/model.wallets";
import { Blockchain_Helper } from "./blockchain.helper";
import { Utility_Helper } from "./utility.helper";

class WalletHelper {
    public tica_pending_balance_update_topic = config.config.KAFKA_BALANCE.TICS_PENDING_BALANCE_UPDATE_TOPIC;

    Fetch_Balance = async (
        address: string,
        coin: CoinInterface,
    ) => {
        try {
            let balance: number | null = null;
            if (coin) {
                switch (coin.coin_family) {
                    case CoinFamilyEnum.TICS:
                        balance = await Blockchain_Helper.Fetch_Balance(address, coin);
                        break;
                    default:
                        balance = 0;
                        break;
                }
            }
            return balance !== null ? Number(await Utility_Helper.exponentialToDecimal(balance)) : null;
        } catch (error: any) {
            console.error(`WalletHelper Fetch_Balance error >>>`, error);
            return null;
        }
    }


    Update_Balance = async (
        wallet_address: string,
        coin: CoinInterface
    ) => {
        try {
            if (!wallet_address) {
                console.error(`wallet_address missing >>> `);
                return false;
            }
            /* fatch wallet balnce */
            let balance: number | null = await this.Fetch_Balance(
                wallet_address,
                coin
            );
            if (balance !== null) {
                let wallet_data: any = await WalletModel.findOne({
                    attributes: ["balance"],
                    where: {
                        wallet_address: wallet_address,
                        coin_id: coin.coin_id,
                    }
                })
                if (wallet_data) {
                    await WalletModel.update({
                        balance: Number(balance),
                        status: 1

                    }, {
                        where: {
                            wallet_address: wallet_address,
                            coin_id: coin.coin_id,
                        }
                    });
                }
            }
            else {
                let data = {
                    coin_data: coin,
                    wallet_address: wallet_address,
                    queue_count: 0,
                }
                await kafkaHelper.produceMessage(this.tica_pending_balance_update_topic, data)
            }
            return balance;
        } catch (error: any) {
            console.error(`WalletHelper Update_Balance error >>>`, error);
            return null;
        }
    }

    Update_all_active_coin_balance = async (wallet_address: string, coin_family: CoinFamilyEnum) => {
        try {
            let address_coins: any = await WalletModel.findAll({
                attributes: ["coin_id", "wallet_address"],
                where: {
                    wallet_address: wallet_address,
                    status: 1,
                    coin_family: coin_family
                },
                include: [{
                    model: CoinsModel,
                    attributes: ["coin_id", "coin_name", "coin_symbol", "coin_family", "is_token", "token_type", "token_address"],
                    as: "coin_data",
                    required: true,
                    where: {
                        coin_status: 1,
                    },
                }],
            })
            for (let i = 0; i < address_coins.length; i++) {
                await this.Update_Balance(address_coins[i].wallet_address, address_coins[i].coin_data);
            }
            return 1;

        } catch (err: any) {
            console.error(`WalletHelper Update_all_active_coin_balance error >>>`, err);
            return 0;

        }
    }

    // update balance queue
    public async fetchBalanceForAddress(wallet_address: string, coin: CoinInterface) {
        try {
            if (!wallet_address) {
                console.error(`wallet_address missing >>> `);
                return false;
            }
            /* fatch wallet balnce */
            let balance: number | null = await this.Fetch_Balance(
                wallet_address,
                coin
            );
            if (balance !== null) {
                return balance;
            }
            return null;
        } catch (err: any) {
            console.error(`WalletHelper updateBalanceQueue error >>>`, err);
            return null;
        }
    }
}
export let Wallet_Helper = new WalletHelper();
