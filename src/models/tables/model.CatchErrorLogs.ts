import { DataTypes, Model, Optional } from "sequelize";
import db from "../../config/db";
import { CatchErrorMsgsInterface } from '../interfaces/index';
interface CatchErrorMsgsCreationModel extends Optional<CatchErrorMsgsInterface, "id"> { }
interface CatchErrorMsgsInstance
    extends Model<CatchErrorMsgsInterface, CatchErrorMsgsCreationModel>,
    CatchErrorMsgsInterface { }

let dataObj = {
    id: {
        type: DataTypes.INTEGER,
        allowNull: false,
        primaryKey: true,
        autoIncrement: true,
    },
    fx_name: {
        type: DataTypes.STRING,
        allowNull: false,
    },
    error_msg: {
        type: DataTypes.JSON,
        allowNull: true
    }
};

const CatchErrorMsgsModel = db.db_write.define<CatchErrorMsgsInstance>(
    "catch_error_logs",
    dataObj
);

export default CatchErrorMsgsModel;