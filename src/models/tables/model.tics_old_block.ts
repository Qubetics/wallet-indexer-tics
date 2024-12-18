import { DataTypes, Model, Optional } from "sequelize";
import { TicsOldBlockInterface } from "../interfaces/index";
import db from "../../config/db";

interface TicsOldBlockCreationModel extends Optional<TicsOldBlockInterface, "id"> { }
interface TicsOldBlockInstance
    extends Model<TicsOldBlockInterface, TicsOldBlockCreationModel>,
    TicsOldBlockInterface { }

let dataObj = {
    id: {
        primaryKey: true,
        type: DataTypes.INTEGER,
        allowNull: false,
        autoIncrement: true,
    },
    block_number: {
        type: DataTypes.INTEGER,
        allowNull: false,
    },
    start_block: {
        type: DataTypes.INTEGER,
        allowNull: false,
    },
    end_block: {
        type: DataTypes.INTEGER,
        allowNull: false,
    },
    coin_family: {
        type: DataTypes.INTEGER,
        allowNull: false,
    },
    status: {
        type: DataTypes.TINYINT,
        allowNull: true,
    }
};
let dataObjIndex = {
    createdAt: 'created_at',
    updatedAt: 'updated_at',
    indexes: [
        {
            unique: false,
            fields: ["status"],
        }
    ]
}


const TicsOldBLockModel = db.db_write.define<TicsOldBlockInstance>("tics_old_blocks", dataObj, dataObjIndex);

export default TicsOldBLockModel;