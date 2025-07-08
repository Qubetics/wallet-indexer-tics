import { config } from "../config";
import kafkaHelper from "../config/kafka.helper";
import { CoinFamilyEnum } from "../enum";
import { CoinInterface, coinsModel } from "../models";
import walletModel from "../models/tables/model.wallets";
import { blockChainHelper } from "./blockchain.helper";
import { utilityHelper } from "./utility.helper";

class WalletHelper {
    public tica_pending_balance_update_topic = config.config.KAFKA_BALANCE.TICS_PENDING_BALANCE_UPDATE_TOPIC;

    fetchBalance = async (
        address: string,
        coin: CoinInterface,
    ) => {
        try {
            let balance: number | null = null;
            if (coin) {
                switch (coin.coin_family) {
                    case CoinFamilyEnum.TICS:
                        balance = await blockChainHelper.fetchBalance(address, coin);
                        break;
                    default:
                        balance = 0;
                        break;
                }
            }
            return balance !== null ? Number(await utilityHelper.exponentialToDecimal(balance)) : null;
        } catch (error: any) {
            console.error(`WalletHelper Fetch_Balance error >>>`, error);
            return null;
        }
    }


    updateBalance = async (
        wallet_address: string,
        coin: CoinInterface
    ) => {
        try {
            if (!wallet_address) {
                console.error(`wallet_address missing >>> `);
                return false;
            }
            /* fatch wallet balnce */
            let balance: number | null = await this.fetchBalance(
                wallet_address,
                coin
            );
            if (balance !== null) {
                let wallet_data: any = await walletModel.findOne({
                    attributes: ["balance"],
                    where: {
                        wallet_address: wallet_address,
                        coin_id: coin.coin_id,
                    }
                })
                if (wallet_data) {
                    await walletModel.update({
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

    updateAllActiveCoinBalance = async (wallet_address: string, coin_family: CoinFamilyEnum) => {
        try {
            let address_coins: any = await walletModel.findAll({
                attributes: ["coin_id", "wallet_address"],
                where: {
                    wallet_address: wallet_address,
                    status: 1,
                    coin_family: coin_family
                },
                include: [{
                    model: coinsModel,
                    attributes: ["coin_id", "coin_name", "coin_symbol", "coin_family", "is_token", "token_type", "token_address"],
                    as: "coin_data",
                    required: true,
                    where: {
                        coin_status: 1,
                    },
                }],
            })
            for (let i = 0; i < address_coins.length; i++) {
                await this.updateBalance(address_coins[i].wallet_address, address_coins[i].coin_data);
            }
            return 1;

        } catch (err: any) {
            console.error(`WalletHelper Update_all_active_coin_balance error >>>`, err);
            return 0;

        }
    }

    // update balance queue
    public async getBalance(wallet_address: string, coin: CoinInterface) {
        try {
            if (!wallet_address) {
                console.error(`wallet_address missing >>> `);
                return false;
            }
            /* fatch wallet balnce */
            let balance: number | null = await this.fetchBalance(
                wallet_address,
                coin
            );
            if (balance !== null) {
                return balance;
            }
            return null;
        } catch (err: any) {
            console.error(`WalletHelper Update_Balance error >>>`, err);
            return null;
        }
    }
}
export let walletHelper = new WalletHelper();
