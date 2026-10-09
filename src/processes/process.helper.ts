import database from "../config/db";
import Sequelize, { QueryTypes } from "sequelize";
import { CoinInterface, CoinsModel, TicsOldBLockModel, WalletInterface } from "../models";
import redisClient from "../config/redis";
import { promisify } from "util";
import { config } from "../config/config";
import { BooleanEnum, CoinFamilyEnum } from "../enum";
import { global_helper } from "../helpers/global_helper";
import { Blockchain_Helper } from "../helpers";
import { Op } from "sequelize";

class TicsProcessHelper {
  public CoinsInterface: CoinInterface[];
  public WalletAddresses: WalletInterface[];

  constructor() {
    this.fetchERC20Tokens();
    this.fetchWalletAddress();
  }
  public async getKeyValuePair(keyName: string, field: string) {
    try {
      const getAsync = promisify(redisClient.HGET).bind(redisClient);
      const value = await getAsync(keyName, field.toUpperCase() || "");
      return value;
    } catch (err: any) {
      console.error('getKeyValuePair >>>>', err);
      return null;
    }
  }
  public async setBlocks(field: string, key: string, minBlockConfirmation: number, newBlock: number | null) {
    try {
      let latestBlock = await Blockchain_Helper.provider.getBlockNumber();
      latestBlock = latestBlock - minBlockConfirmation;
      if (newBlock) {
        if (newBlock < latestBlock) {
          await ticsProcessHelper.setKeyValuePair(field, key, latestBlock.toString())
        } else {
          console.log("No updation of blocks")
        }
      } else {
        await ticsProcessHelper.setKeyValuePair(field, key, latestBlock.toString())
      }
    } catch (err: any) {
      console.error("Error in  getBlocks", err)
      await global_helper.save_error_logs('TICS_setBlocks', err.message)
    }
  }
  public async setKeyValuePair(keyName: string, field: string, value: string) {
    try {
      redisClient.hset(keyName, field.toUpperCase(), value);
    } catch (err: any) {
      console.error('setKeyValuePair >>>', err);
    }
  }

  public async diff_blocks_add_in_db(current_block: number, old_block: number) {
    try {
      let value_exist: any = await this.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.NODE_ALERT_TICS)
      if (value_exist) {
        let setvalue: number = Number(value_exist) + 1;
        if (Number(value_exist) <= 100) {
          await this.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.NODE_ALERT_TICS, setvalue.toString())
          if (Number(value_exist) == 0 || Number(value_exist) == 5 || Number(value_exist) == 10 || Number(value_exist) == 20 || Number(value_exist) == 50 || Number(value_exist) == 75 || Number(value_exist) == 100) {
            let email_body = `<p>Qubetics Node Issue ${Number(value_exist)}</p><p>Current block running = ${current_block}.</p><p>Old Block = ${old_block}.</p>`;
            await global_helper.send_mail(config.NODE_ISSUE_ALERT_EMAIL, email_body, `Qubetics Node Issue ${Number(value_exist)}`);
          } else {
            console.log("no email")
          }
        } else {
          console.log("Number(value_exist) > 100")
        }
      } else {
        await this.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.NODE_ALERT_TICS, '0')
      }
      let dataToInsert = { block_number: old_block, coin_family: config.STATIC_COIN_FAMILY.TICS, start_block: old_block, end_block: current_block, status: 0 }
      console.log("dataToInsert>>>>", dataToInsert)
      await TicsOldBLockModel.create(dataToInsert)
      return true;
    } catch (err: any) {
      console.error("💥 ~ Error in diff_blocks_add_in_redis.", err)
      await global_helper.save_error_logs('TICS_diff_blocks_add_in_db', err.message)
      return false;
    }
  }


  public async deleteKeyValuePair(keyName: string, field: string) {
    try {
      redisClient.hdel(keyName, field.toUpperCase(), function (err) {
        if (err) {
          throw err;
        }
        return true;
      })
    } catch (err: any) {
      console.error("Error in deleteKeyValuePair>>>", err)
      return false;
    }
  }


  public fetchERC20Tokens = async () => {
    try {
      let tokens: any = await CoinsModel.findAll({
        attributes: ["coin_id", "coin_name", "coin_symbol", "coin_image", "coin_family", "coin_status", "is_token", "token_type", "decimals", "cmc_id", "is_on_cmc", "token_abi", "token_address", "for_swap", "added_by", "created_at", "updated_at"],
        where: {
          coin_status: 1,
          coin_family: config.STATIC_COIN_FAMILY.TICS,
          //[Op.or]: [{ token_type: { [Op.ne]: config.TOKEN_TYPE721_TICS } }, { token_type: null }]
        },
        raw: true
      })
      redisClient.set(config.TOKEN_TYPE_TICS || "", JSON.stringify(tokens));
    } catch (err: any) {
      console.error('fetchERC20Tokens >>>>>', err);
      await global_helper.save_error_logs('TICS_fetchERC20Tokens', err.message)
    }
  }

  public fetchWalletAddress = async () => {
    try {
      const getaddressQuery = `SELECT ws.user_id,ws.wallet_address as address, ws.wallet_name FROM wallets ws FORCE INDEX (coin_family) JOIN coins cs ON ws.coin_id = cs.coin_id WHERE cs.coin_family = ${config.STATIC_COIN_FAMILY.TICS} GROUP BY ws.wallet_address`;
      const addresses: any = await database.db_write.query(
        getaddressQuery,
        { type: QueryTypes.SELECT }
      );
      for await (let wallet of addresses) {
        let findAddress = await this.getKeyValuePair(
          `${config.TICS_WALLET_ADDRESS}`,
          wallet.address.toUpperCase()
        );
        if (!findAddress) {
          this.setKeyValuePair(
            `${config.TICS_WALLET_ADDRESS}`,
            wallet.address.toUpperCase(),
            JSON.stringify(wallet)
          );
        }
      }
    } catch (error) {
      console.error('💥 ~ fetchWalletAddress error', error);
    }
  };

  public checkIfContract = async (address: string) => {
    try {
      const getAsync = promisify(redisClient.get).bind(redisClient);
      const value = await getAsync(config.TOKEN_TYPE_TICS || "");

      const erc20Tokens: CoinInterface[] = value ? JSON.parse(value) : [];
      const token = erc20Tokens.find(
        (el: any) => Number(el.is_token) === Number(BooleanEnum.true) && el.token_address?.toUpperCase() === address?.toUpperCase()
      );
      if (token) return token;
      return null;
    } catch (error: any) {
      console.error('checkIfContract >>>', error);
      return null;
    }
  };


  public check_our_wallet_address = async (address: string) => {
    try {
      let wallet_data: any = await this.getKeyValuePair(config.TICS_WALLET_ADDRESS, address);
      if (wallet_data) {
        console.log("wallet_Data>>>>>>>>", wallet_data)
      }
      return JSON.parse(wallet_data);
    } catch (error: any) {
      console.error('check_our_wallet_address >>>>>', error);
      return null;
    }
  };
  public NativeCoinByCoinFamily = async (
    coin_family: CoinFamilyEnum
  ) => {
    try {
      const getAsync = promisify(redisClient.get).bind(redisClient);
      const value = await getAsync(config.TOKEN_TYPE_TICS || "");

      const erc20Tokens: CoinInterface[] = value ? JSON.parse(value) : [];
      return erc20Tokens.find(
        (el: CoinInterface) => Number(el.is_token) === Number(BooleanEnum.false) && el.coin_family === coin_family
      );

    } catch (error: any) {
      console.error(`NativeCoinByCoinFamily >>>`, error);
      return 0;
    }
  }

  public CoinByCoinId = async (
    coin_id: number
  ) => {
    try {
      const getAsync = promisify(redisClient.get).bind(redisClient);
      const value = await getAsync(config.TOKEN_TYPE_TICS || "");

      const erc20Tokens: CoinInterface[] = value ? JSON.parse(value) : [];
      return erc20Tokens.find(
        (el: CoinInterface) => el.coin_id === coin_id
      );

    } catch (error: any) {
      console.error(`CoinByCoinId >>>`, error);
      return null;
    }
  }
  public behindBlocks = async () => {
    try {
      let behindBlocks: any = await TicsOldBLockModel.findOne({
        attributes: ['id', 'block_number', 'start_block', 'end_block'],
        where: { status: 0 },
        raw: true,
        order: [['id', 'ASC']]
      })
      if (behindBlocks) {
        let add_data = {
          id: behindBlocks.id,
          start_block: behindBlocks.start_block,
          end_block: behindBlocks.end_block,
          diff: behindBlocks.end_block - behindBlocks.start_block
        }
        let dataaaaa_exist: any = await this.setKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_TICS, JSON.stringify(add_data))
      } else {
        console.log("you have not any behind block yet")
      }
    } catch (err: any) {
      console.error(`behindBlocks >>>`, err);
      return null;
    }
  }
}
const ticsProcessHelper = new TicsProcessHelper();
export default ticsProcessHelper;