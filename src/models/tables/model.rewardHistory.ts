// import { DataTypes, Model, Optional } from "sequelize";
// import { IRewardHistoryInterface } from "../interfaces/interface.reward_history";
// import db from "../../config/db";
// interface RewardHistoryCreationModel extends Optional<IRewardHistoryInterface, "id"> { }
// interface RewardHistoryInstance
//     extends Model<IRewardHistoryInterface, RewardHistoryCreationModel>,
//     IRewardHistoryInterface { }

// let dataObj = {
//     id: {
//         type: DataTypes.INTEGER,
//         allowNull: false,
//         primaryKey: true,
//         autoIncrement: true
//     },
//     user_id: {
//         type: DataTypes.INTEGER,
//         allowNull: false
//     },
//     tx_id: {
//         type: DataTypes.INTEGER,
//         allowNull: true,
//     },
//     address: {
//         type: DataTypes.STRING,
//         allowNull: true
//     },
//     currency: {
//         type: DataTypes.STRING,
//         allowNull: false
//     },
//     amount: {
//         type: DataTypes.STRING,
//         allowNull: true
//     },
//     percentage: {
//         type: DataTypes.STRING,
//         allowNull: true
//     },
//     status: {
//         type: DataTypes.ENUM('0', '1', '2'),
//         allowNull: true,
//         defaultValue: "0"
//     },
//     type: {
//         type: DataTypes.STRING,
//         allowNull: false
//     }
// };

// let dataObjIndex = {
//     createdAt: 'created_at',
//     updatedAt: 'updated_at'
// }

// const RewardHistoryModel = db.db_write.define<RewardHistoryInstance>(
//     "reward_histories",
//     dataObj,
//     dataObjIndex
// );
// export default RewardHistoryModel;