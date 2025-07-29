import BigNumber from "bignumber.js";
import {
    Db_Helper
} from ".";

import { NotificationModel } from "../models";
import firebaseAdmin from 'firebase-admin';
import { fcmServiceAccount } from "../constants/fcm.admin.key";
import { Op } from "sequelize";

class UtilityHelper {
    public messaging: firebaseAdmin.messaging.Messaging;
    constructor() {
        firebaseAdmin.initializeApp({
            credential: firebaseAdmin.credential.cert(fcmServiceAccount),
        });
        this.messaging = firebaseAdmin.messaging();
    }

    bigNumberSafeMath = async (c: any, operation: any, d: any) => {
        18;
        var a = new BigNumber(c);
        var b = new BigNumber(d);
        var rtn: any;
        switch (operation.toLowerCase()) {
            case "-":
                rtn = a.minus(b);
                break;
            case "+":
                rtn = a.plus(b);
                break;
            case "*":
            case "x":
                rtn = a.multipliedBy(b);
                break;
            case "÷":
            case "/":
                rtn = a.dividedBy(b);
                break;
            default:
                break;
        }
        return rtn.toString();
    }

    bigNumberSafeConversion = async (val: number) => {
        const amount = val.toString();
        const value = new BigNumber(amount);
        return value.toFixed();
    };

    convertAmount = async (hexAmount: any) => {
        return new BigNumber("0x" + hexAmount.toString());
    }


    exponentialToDecimal = async (exponential: number) => {
        let decimal: string = exponential.toString().toLowerCase();
        if (decimal.includes("e+")) {
            const exponentialSplitted = decimal.split("e+");
            let postfix: string = "";
            for (let i = 0; i < +exponentialSplitted[1] - (exponentialSplitted[0].includes(".") ? exponentialSplitted[0].split(".")[1].length : 0); i++) {
                postfix += "0";
            }
            const addCommas = (text: string) => {
                let j: number = 3;
                let textLength: number = text.length;
                while (j < textLength) {
                    text = `${text.slice(0, textLength - j)}${text.slice(textLength - j, textLength)}`;
                    textLength++;
                    j += 3 + 1;
                }
                return text;
            };
            decimal = addCommas(exponentialSplitted[0].replace(".", "") + postfix);
        }
        if (decimal.toLowerCase().includes("e-")) {
            const exponentialSplitted = decimal.split("e-");
            let prefix: string = "0.";
            for (let i = 0; i < +exponentialSplitted[1] - 1; i++) {
                prefix += "0";
            }
            decimal = prefix + exponentialSplitted[0].replace(".", "");
        }
        return decimal;
    }

    public async SendNotification(data: any) {
        try {
            let toUserId: number = data.to_user_id ? data.to_user_id : 0;
            let deviceTokens: any = await Db_Helper.GetDeviceTokens(toUserId, data.wallet_address, data.coin_family);
            let finalDeviceTokens: any = [];

            for await (let deviceToken of deviceTokens) {
                finalDeviceTokens.push(deviceToken.device_token);
            }

            if (finalDeviceTokens?.length > 0) {
                let announcement_title: string = data.announcement_title == undefined ? "" : data.announcement_title;
                let announcement_message: string = data.announcement_message == undefined ? "" : data.announcement_message;
                const notification = {
                    title: data.title,
                    body: data.message,
                };

                this.messaging
                    .sendEachForMulticast({
                        tokens: finalDeviceTokens,
                        notification: notification,
                        data: {
                            body: data.message ? data.message : "",
                            title: data.title ? data.title : "",
                            notification_type: data.notification_type ? data.notification_type : "",
                            tx_id: data.tx_id ? data.tx_id?.toString() : "0",
                            tx_type: data.tx_type ? data.tx_type : "",
                            from_user_id: data.from_user_id ? data.from_user_id?.toString() : "0",
                            user_coin_id: data.coin_id ? data.coin_id?.toString() : "0",
                            announcement_title: announcement_title,
                            announcement_message: announcement_message,
                        }
                    })
                    .then((response: any) => {
                        console.log("💥🚀 ~ firebase admin push notification", JSON.stringify(response));
                    })
                    .catch((error: any) => {
                        console.error(
                            "💥 ~ firebase admin sending notification error",
                            error
                        );
                    });
            }

            /* save notification */
            const checkOldNotif: any = await NotificationModel.count({
                where: {
                    wallet_address: { [Op.like]: data.wallet_address },
                    notification_type: data.notification_type,
                    tx_id: data.tx_id,
                },
            });
            if (checkOldNotif == 0) {
                let notificationModel: any = {
                    message: data.message,
                    from_user_id: data.from_user_id ? data.from_user_id : 0,
                    amount: data.amount,
                    to_user_id: toUserId,
                    coin_symbol: data.coin_symbol,
                    notification_type: data.notification_type,
                    tx_id: data.tx_id,
                    coin_id: data.coin_id,
                    wallet_address: data.wallet_address,
                    coin_family: data.coin_family,
                    tx_type: data.tx_type,
                    view_status: "0",
                    state: "0",
                };
                await NotificationModel.create(notificationModel);
            }
            return true;
        } catch (error) {
            console.error(`🔥 ~ sendNewNotification utility.helper.ts error`, error);
            return false;
        }
    }




}
export let Utility_Helper = new UtilityHelper();
