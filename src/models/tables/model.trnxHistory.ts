import { DataTypes, Model, Optional } from "sequelize";
import { ITrnxHistoryInterface } from "../interfaces/interface.trnxHistory";
import db from "../../config/db";
import CoinsModel from "./model.coins";


interface TrnxHistoryCreationModel extends Optional<ITrnxHistoryInterface, "id"> { }
interface TrnxHistoryInstance
    extends Model<ITrnxHistoryInterface, TrnxHistoryCreationModel>,
    ITrnxHistoryInterface { }

let dataObj = {
    id: {
        type: DataTypes.INTEGER,
        allowNull: false,
        primaryKey: true,
        autoIncrement: true
    },
    user_id: {
        type: DataTypes.INTEGER,
        allowNull: false
    },
    to_user_id: {
        type: DataTypes.INTEGER,
        allowNull: true
    },
    coin_id: {
        type: DataTypes.INTEGER,
        allowNull: false
    },
    coin_family: {
        type: DataTypes.TINYINT,
        allowNull: false
    },
    type: {
        type: DataTypes.STRING,
        allowNull: false
    },
    req_type: {
        type: DataTypes.ENUM('APP', 'EXNG'),
        allowNull: false
    },
    from_adrs: {
        type: DataTypes.STRING,
        allowNull: true
    },
    to_adrs: {
        type: DataTypes.STRING,
        allowNull: false
    },
    tx_id: {
        type: DataTypes.STRING,
        allowNull: true
    },
    merchant_id: {
        type: DataTypes.STRING,
        allowNull: true
    },
    order_id: {
        type: DataTypes.STRING,
        allowNull: true
    },
    tx_raw: {
        type: DataTypes.TEXT,
        allowNull: true
    },
    status: {
        type: DataTypes.STRING,
        allowNull: false
    },
    blockchain_status: {
        type: DataTypes.ENUM('pending', 'failed', 'confirmed'),
        allowNull: true
    },
    amount: {
        type: DataTypes.DOUBLE,
        allowNull: false
    },
    block_id: {
        type: DataTypes.INTEGER,
        allowNull: true
    },
    nonce: {
        type: DataTypes.INTEGER,
        allowNull: true
    },
    tx_fee: {
        type: DataTypes.DOUBLE,
        allowNull: true
    },
    swap_fee: {
        type: DataTypes.DOUBLE,
        allowNull: true
    },
    gas_limit: {
        type: DataTypes.INTEGER,
        allowNull: true
    },
    gas_price: {
        type: DataTypes.DOUBLE,
        allowNull: true
    },
    gas_reverted: {
        type: DataTypes.DOUBLE,
        allowNull: true
    },
    fiat_price: {
        type: DataTypes.DOUBLE,
        allowNull: true
    },
    fiat_type: {
        type: DataTypes.STRING(191),
        allowNull: true
    },
};

let dataObjIndex = {
    createdAt: 'created_at',
    updatedAt: 'updated_at'
}

const TrnxHistoryModel = db.db_write.define<TrnxHistoryInstance>(
    "trnx_histories",
    dataObj,
    dataObjIndex
);
TrnxHistoryModel.belongsTo(CoinsModel, { foreignKey: 'coin_id', targetKey: 'coin_id', as: "coin_data" });

export default TrnxHistoryModel;