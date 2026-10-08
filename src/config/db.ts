import { DBInterface } from "../interfaces/db.interface";
import { Sequelize } from "sequelize";
import { config } from "./config";

import "dotenv/config";

class Database implements DBInterface {
  public db_write: Sequelize;
  public db_read: Sequelize;
  public connectionWrite: string;
  public connectionRead: string;

  constructor() {
    this.connectionWrite = `mysql://${config.DB_USER}:${config.DB_PASSWORD}@${config.DB_HOST_WRITE}/${config.DB_NAME}`;
    this.connectionRead = `mysql://${config.DB_USER}:${config.DB_PASSWORD}@${config.DB_HOST_READ}/${config.DB_NAME}`;


    this.db_write = new Sequelize(this.connectionWrite, {
      dialect: "mysql",
      logging: false,
      define: {
        charset: "utf8",
        collate: "utf8_general_ci",
        underscored: true,
        timestamps: true,
      },
      pool: {
        max: 5,
        min: 0,
        idle: 20000,
        acquire: 60000,
      },
    });

    this.db_read = new Sequelize(this.connectionRead, {
      dialect: "mysql",
      logging: false,
      define: {
        charset: "utf8",
        collate: "utf8_general_ci",
        underscored: true,
        timestamps: true,
      },
      pool: {
        max: 5,
        min: 0,
        idle: 20000,
        acquire: 60000,
      },
    });
    this.db_read
      .authenticate()
      .then(() => {
        console.debug("Database connected successfully.");
      })
      .catch((error: any) => {
        console.error(`Unable to connect to the database: ${config.DB_NAME}`, error);
      });
  }

}
const db = new Database();
export default {
  db_write: db.db_write,
  db_read: db.db_read,
};
