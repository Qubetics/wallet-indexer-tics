import BigNumber from "bignumber.js";
// import FCM from 'fcm-push';
// /import FCM from 'fcm-node'
// var FCM = require("fcm-node");


import {
    Db_Helper
} from ".";

// import {
//     config
// } from "../config/config";

import { NotificationModel } from "../models";
// const firebaseAdmin = require("firebase-admin");
// import fcmServiceAccount from "../constants/fcm.admin.key.json";
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
        // (async () => {
        //     await firebaseAdmin.initializeApp({
        //         credential: firebaseAdmin.credential.cert(fcmServiceAccount),
        //     });
        // })();
    }
    //    public REDIS_BLOCKS: string = `${CONFIG.REDISKEYS.BLOCKS}_${CONFIG.COIN_FAMILY_TICS}`;
    // public FCM_SERVER_KEY: any = config.KEYS.FCM_PUSH;

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
                //operator = operation;
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

    //    UpdateBlockNumber = async (
    //       block_number: number
    //    ) => {
    //       Redis_Helper.SetHashTable(
    //          this.REDIS_BLOCKS, // key
    //          BlocksKeysEnum.OLD_READ,// field
    //          JSON.stringify(Number(block_number)) // value 
    //       );

    //       /** next block to read */
    //       Redis_Helper.SetHashTable(
    //          this.REDIS_BLOCKS, // key
    //          BlocksKeysEnum.NEXT_READ,// field
    //          JSON.stringify(++block_number) // value 
    //       );
    //       return true;
    //    }

    // SendNotificationold = async (
    //     data: any
    // ) => {
    //     try {
    //         let toUserId: number = data.to_user_id ? data.to_user_id : 0;

    //         let deviceTokens: any = await Db_Helper.GetDeviceTokens(toUserId);
    //         let device_tokens = [];

    //         for await (let deviceToken of deviceTokens) {
    //             device_tokens.push(deviceToken.device_token);
    //         }

    //         if (device_tokens && this.FCM_SERVER_KEY) {
    //             let announcement_title: string = data.announcement_title == undefined ? "" : data.announcement_title;
    //             let announcement_message: string = data.announcement_message == undefined ? "" : data.announcement_message;
    //             let fcm = new FCM(this.FCM_SERVER_KEY);
    //             let message;
    //             if (Array.isArray(device_tokens)) {
    //                 message = {
    //                     registration_ids: device_tokens,
    //                     collapse_key: "type_a",
    //                     notification: {
    //                         title: data.title,
    //                         body: data.message,
    //                         sound: "default",
    //                     },
    //                     data: {
    //                         body: data.message,
    //                         title: data.title,
    //                         notification_type: data.notification_type,
    //                         tx_id: data.tx_id ? data.tx_id : 0,
    //                         tx_type: data.tx_type,
    //                         from_user_id: data.from_user_id ? data.from_user_id : 0,
    //                         user_coin_id: data.coin_id ? data.coin_id : 0,
    //                         announcement_title: announcement_title,
    //                         announcement_message: announcement_message,
    //                     },
    //                 };
    //             } else {
    //                 message = {
    //                     to: device_tokens,
    //                     collapse_key: "type_a",
    //                     notification: {
    //                         title: data.title,
    //                         body: data.message,
    //                         sound: "default",
    //                     },
    //                     data: {
    //                         body: data.message,
    //                         title: data.title,
    //                         notification_type: data.notification_type,
    //                         tx_id: data.tx_id ? data.tx_id : 0,
    //                         tx_type: data.tx_type,
    //                         from_user_id: data.from_user_id ? data.from_user_id : 0,
    //                         user_coin_id: data.coin_id ? data.coin_id : 0,
    //                         announcement_title: announcement_title,
    //                         announcement_message: announcement_message,
    //                     },
    //                 };
    //             }
    //             fcm.send(message, function (err: any, messageId: number) {
    //                 if (err) {
    //                     console.error("Something has gone wrong!", err);
    //                 }
    //                 console.debug(`fcm push notification >>`, messageId);
    //             });
    //         }
    //         const checkOldNotif: any = await NotificationModel.count({
    //             where: {
    //                 to_user_id: toUserId,
    //                 notification_type: data.notification_type,
    //                 tx_id: data.tx_id,
    //                 tx_type: data.tx_type
    //             }
    //         });
    //         if (checkOldNotif == 0) {
    //             let notificationModel: any = {
    //                 message: data.message,
    //                 from_user_id: data.from_user_id ? data.from_user_id : 0,
    //                 amount: data.amount,
    //                 to_user_id: toUserId,
    //                 coin_symbol: data.coin_symbol,
    //                 notification_type: data.notification_type,
    //                 tx_id: data.tx_id,
    //                 coin_id: data.coin_id,
    //                 wallet_address: data.wallet_address,
    //                 tx_type: data.notification_type,
    //                 view_status: "0",
    //                 state: "0",
    //             }
    //             await NotificationModel.create(notificationModel);
    //         }
    //         return true;
    //     } catch (error: any) {
    //         console.error(`SendNotification error >>>`, error);
    //         return false;
    //     }
    // };

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
                // const messaging = firebaseAdmin.messaging();
                const notification = {
                    title: data.title,
                    body: data.message,
                };

                this.messaging
                    .sendEachForMulticast({
                        tokens: finalDeviceTokens,
                        // collapse_key: "type_a",
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

                // if (Array.isArray(finalDeviceTokens)) {
                //     messaging.sendMulticast({ tokens: finalDeviceTokens, notification: notification})
                //         .then((response: any) => {
                //             console.log("💥🚀 ~ firebase admin push notification", response);
                //         })
                //         .catch((error: any) => {
                //             console.error("💥 ~ firebase admin sending notification error", error);
                //         });
                // } else {
                //     messaging.send({ token: finalDeviceTokens, notification: notification })
                //         .then((response: any) => {
                //             console.log("💥🚀 ~ firebase admin push notification", response);
                //         }).catch((error: any) => {
                //             console.error("💥 ~ firebase admin sending notification error", error);
                //         });
                // }
            }

            /* save notification */
            const checkOldNotif: any = await NotificationModel.count({
                where: {
                    wallet_address: { [Op.like]: data.wallet_address },
                    notification_type: data.notification_type,
                    tx_id: data.tx_id,
                    // tx_type: data.tx_type,
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
                    tx_type: data.notification_type,
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
