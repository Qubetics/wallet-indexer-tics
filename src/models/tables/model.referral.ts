// import { DataTypes, Model, Optional } from "sequelize";
// import { ReferralInterface } from "../interfaces/interface.referrals";
// import db from "../../config/db";

// interface ReferralModel extends Optional<ReferralInterface, "id"> { }
// interface ReferralInstance
//     extends Model<ReferralInterface, ReferralModel>,
//     ReferralInterface { }

// let dataObj: any = {
//     id: {
//         type: DataTypes.INTEGER,
//         allowNull: false,
//         primaryKey: true,
//         autoIncrement: true,
//     },
//     referrer_from: {
//         type: DataTypes.INTEGER,
//         allowNull: false
//     },
//     referrer_to: {
//         type: DataTypes.INTEGER,
//         allowNull: false
//     },
//     used_referral_code: {
//         type: DataTypes.STRING,
//         allowNull: false
//     },
//     to_device_id: {
//         type: DataTypes.STRING,
//         allowNull: false
//     }
// };

// const ReferralModel = db.db_write.define<ReferralInstance>("referrals", dataObj);

// export default ReferralModel;


