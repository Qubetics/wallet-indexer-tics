import {
    BooleanEnum,
} from "../enum";

import {
    deviceTokenModel,

} from "../models/index";
import { Op } from "sequelize";

class DbHelper {


    /** list of all active crypto coins */

    /** list of all crypto coins with token  */

    /** get user device token using user id */
    getDeviceTokens = async (address: string, coin_family: number) => {
        try {
            return await deviceTokenModel.findAll({
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


}
export let dbHelper = new DbHelper();

