import {
    BooleanEnum,
} from "../enum";

import {
    DeviceTokenModel,
    SettingsModel,
    RewardHistoryModel,
    WalletModel,
    ReferralModel,
    CatchErrorMsgsModel,
    CoinPriceInFiatModel,

} from "../models/index";
import { Utility_Helper } from "../helpers";
import { Op } from "sequelize";

class DbHelper {





    /** get user device token using user id */
    GetDeviceTokens = async (user_id: number, address: string, coin_family: number) => {
        try {
            return await DeviceTokenModel.findAll({
                attributes: ["device_token"],
                where: {
                    address: {
                        [Op.like]: address,
                    },
                    coin_family: coin_family,
                    push: BooleanEnum.true
                },
                order: [
                    ["updated_at", "DESC"],
                    ['id', 'DESC']
                ],
                raw: true
            });
        } catch (error: any) {
            console.error(`getDeviceToken error >>>`, error);
            return false;
        }
    };

    /** save reward data */
    save_reward = async (data: any) => {
        try {
            console.log('save_reward data >>>>>>>>>>>>>>...', data);
            let user_id = data.user_id;
            let fiat_type = data.fiat_type;
            let referral_data: any = await ReferralModel.findOne({ attributes: ['referrer_from'], where: { referred_to: user_id }, raw: true });
            console.log('referral_data >>>', referral_data);
            if (referral_data) {
                let referrer_from = referral_data.referrer_from;
                let wallet_data = await WalletModel.findOne({ attributes: ['wallet_address'], where: { user_id: referrer_from }, raw: true });
                console.log('referral_data wallet_data >>>', wallet_data);
                if (wallet_data) {
                    let address = wallet_data.wallet_address;
                    let reward_setting_data = await SettingsModel.findOne({ attributes: ['value'], where: { title: 'REWARD_PERCENTAGE' }, raw: true });
                    console.log('reward_setting_data >>>', reward_setting_data);
                    if (reward_setting_data) {
                        let reward_already_exist = await RewardHistoryModel.findOne({ attributes: ['id', 'tx_id'], where: { tx_id: data.tx_id } });
                        console.log('reward_already_exist >>>', reward_already_exist);
                        if (!reward_already_exist) {
                            console.log('save history in >>>');
                            let percentage = reward_setting_data?.value;
                            console.log('percentage >>>', percentage);
                            let amount_in_usd = data.amount_in_fiat;

                            if (fiat_type.toUpperCase() !== 'USD' && data.cmc_id !== null) {//10 usd 0.1
                                let cmc_id = data.cmc_id;

                                /** get current fiat price of swap crypto currency */
                                let currency_fiat_data = await CoinPriceInFiatModel.findOne({ attributes: ['value'], where: { cmc_id: cmc_id, fiat_type: fiat_type }, raw: true });
                                let amount_in_fiat = data.amount_in_fiat;
                                if (currency_fiat_data) {
                                    let current_fiat_value = currency_fiat_data.value;
                                    amount_in_fiat = Utility_Helper.bigNumberSafeMath(amount_in_fiat, '/', current_fiat_value);
                                }

                                /*** get usd price of current swap currency */
                                let fiat_coin_data = await CoinPriceInFiatModel.findOne({ attributes: ['value'], where: { cmc_id: cmc_id, fiat_type: 'USD' }, raw: true });
                                if (fiat_coin_data) {
                                    let fiat_value = fiat_coin_data.value;
                                    amount_in_usd = Utility_Helper.bigNumberSafeMath(amount_in_fiat, '*', fiat_value);
                                }

                                console.log('amount_in_usd >>>', amount_in_usd)
                                let amount_percent = await Utility_Helper.bigNumberSafeMath(amount_in_usd, '*', percentage);
                                console.log('amount_percent >>>', amount_percent);
                                let exact_percent_amount = await Utility_Helper.bigNumberSafeMath(amount_percent, '/', 100);
                                console.log('exact_percent_amount >>>', exact_percent_amount)

                                let reward_data: any = {
                                    tx_id: data.tx_id,
                                    address: address,
                                    amount: exact_percent_amount,
                                    percentage: percentage,
                                    currency: 'usd',
                                    type: data.type,
                                    user_id: referrer_from
                                }
                                console.log('reward_data >>>', reward_data)
                                await RewardHistoryModel.create(reward_data);
                                return true;
                            } else if (fiat_type.toUpperCase() == 'USD') {
                                console.log('amount_in_usd >>>', amount_in_usd)
                                let amount_percent = await Utility_Helper.bigNumberSafeMath(amount_in_usd, '*', percentage);
                                console.log('amount_percent >>>', amount_percent)

                                let exact_percent_amount = await Utility_Helper.bigNumberSafeMath(amount_percent, '/', 100);
                                console.log('exact_percent_amount >>>', exact_percent_amount)
                                let reward_data: any = {
                                    tx_id: data.tx_id,
                                    address: address,
                                    amount: exact_percent_amount,
                                    percentage: percentage,
                                    currency: 'usd',
                                    type: data.type,
                                    user_id: referrer_from
                                }
                                console.log('reward_data >>>', reward_data)
                                await RewardHistoryModel.create(reward_data);
                                return true;
                            } else {
                                let error_msg = {
                                    fiat_type: fiat_type || '',
                                    cmc_id: data.cmc_id || '',
                                    tx_id: data.tx_id || ''
                                }
                                await CatchErrorMsgsModel.create({
                                    fx_name: 'save_reward',
                                    error_msg: error_msg
                                })
                            }
                        }
                    }
                }
            }
        } catch (error: any) {
            console.error(`save_rewards error >>>`, error);
            let error_msg = {
                fiat_type: error.message || ''
            }
            await CatchErrorMsgsModel.create({
                fx_name: 'save_reward',
                error_msg: error_msg
            })
        }
    }
}
export let Db_Helper = new DbHelper();

