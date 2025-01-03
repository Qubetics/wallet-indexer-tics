import { DataTypes, Model, Optional } from "sequelize";
import { WalletInterface } from "../interfaces/index";
import db from "../../config/db";
import CoinsModel from "./model.coins";
interface WalletCreationModel extends Optional<WalletInterface, "wallet_id"> { }
interface WalletInstance
  extends Model<WalletInterface, WalletCreationModel>,
  WalletInterface { }

let dataObj = {
  wallet_id: {
    type: DataTypes.INTEGER,
    allowNull: false,
    primaryKey: true,
    autoIncrement: true,
  },
  user_id: {
    type: DataTypes.INTEGER,
    allowNull: false,
  },
  // email: {
  //   type: DataTypes.STRING,
  //   allowNull: true,
  // },
  wallet_name: {
    type: DataTypes.STRING,
    allowNull: false,
  },
  wallet_address: {
    type: DataTypes.STRING,
    allowNull: false,
  },
  coin_id: {
    type: DataTypes.INTEGER,
    allowNull: false,
  },
  coin_family: {
    type: DataTypes.TINYINT,
    allowNull: false,
  },
  balance: {
    type: DataTypes.DOUBLE,
    allowNull: false,
  },
  balance_blocked: {
    type: DataTypes.DOUBLE,
    allowNull: true,
  },
  user_withdraw_limit: {
    type: DataTypes.FLOAT,
    allowNull: true,
  },
  default_wallet: {
    type: DataTypes.TINYINT,
    allowNull: true,
  },
  is_verified: {
    type: DataTypes.TINYINT,
    allowNull: true,
  },
  status: {
    type: DataTypes.TINYINT,
    allowNull: true,
  },
  is_deleted: {
    type: DataTypes.TINYINT,
    allowNull: true,
  },
  sort_order: {
    type: DataTypes.INTEGER,
    allowNull: true,
  },
  is_private_wallet: {
    type: DataTypes.TINYINT,
    allowNull: true,
  },
  // created_at: {
  //   type: DataTypes.DATE,
  //   allowNull: false,
  //   field: "created_at",
  // },
  // updated_at: {
  //   type: DataTypes.DATE,
  //   allowNull: false,
  //   field: "updated_at",
  // },
};

let dataObjIndex = {
  indexes: [
    {
      unique: false,
      fields: ["user_id"],
    },
    {
      unique: false,
      fields: ["wallet_address"],
    },
    {
      unique: false,
      fields: ["coin_id"],
    },
    {
      unique: false,
      fields: ["default_wallet"],
    },
    {
      unique: false,
      fields: ["is_verified"],
    },
    {
      unique: false,
      fields: ["status"],
    },
  ],
};

const WalletModel = db.db_write.define<WalletInstance>(
  "wallets",
  dataObj,
  dataObjIndex
);

WalletModel.belongsTo(CoinsModel, {
  foreignKey: "coin_id",
  targetKey: "coin_id",
  as: "coin_data"
});



export default WalletModel;