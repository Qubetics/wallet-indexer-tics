// import { DataTypes, Model, Optional } from "sequelize";
// import { SettingsInterface } from "../interfaces/index";
// import db from "../../config/db";


// interface SettingsCreationModel extends Optional<SettingsInterface, "id"> {}
// interface SettingsInstance
//   extends Model<SettingsInterface, SettingsCreationModel>,
//   SettingsInterface {}

// const dataObj = {
//   id: {
//     type: DataTypes.INTEGER,
//     allowNull: false,
//     primaryKey: true,
//     autoIncrement: true,
//   },
//   title: {
//     type: DataTypes.STRING,
//     allowNull: false,
//   },
//   value: {
//     type: DataTypes.STRING,
//     allowNull: true,
//   },
//   created_at:{
//     type:DataTypes.DATE,
//     defaultValue:DataTypes.NOW
// },
// updated_at:{
//     type:DataTypes.DATE,
//     defaultValue:DataTypes.NOW
// }
// };

// const SettingsModel = db.db_write.define<SettingsInstance>(
//   "settings",
//   dataObj
// );


// export default SettingsModel;