import { Blockchain_Helper } from "../helpers/index";
import { config } from "../config/config";
import ticsProcessHelper from "./process.helper";
import { KEYS } from "../enum";
import { blocks_trnx } from "../interfaces/global_helper";
// import rabbitMq from "../config/rabbitMq";
import { TicsOldBLockModel } from "../models";
import { global_helper } from "../helpers/global_helper";
import kafkaHelper from "../config/kafka.helper";


class Tics_block_process {
    public minBlockConfirmation: number;
    public swap: string | null;
    public approval: string | null;
    public tics_block_tx_topic = config.KAFKA_BLOCKS.TICS_BLOCK_TX_TOPIC;
    public tics_behind_block_tx_topic = config.KAFKA_BLOCKS.TICS_BEHIND_BLOCK_TX_TOPIC;

    constructor() {
        this.minBlockConfirmation = Number(config.MIN_BLOCK_CONFORMATIONS);
        this.swap = Blockchain_Helper.Web3.utils.sha3('Swap(address,uint256,uint256,uint256,uint256,address)');
        this.approval = Blockchain_Helper.Web3.utils.sha3('Approval(address,address,uint256)');
    }

    public async getBlocks() {
        try {
            /** Testing */
            // let { getBlockNumber } = Blockchain_Helper.Web3.eth;
            // let latestBlock = await getBlockNumber();
            // console.log("latestBlock>>", latestBlock)
            // return;
            let old_block: any = await ticsProcessHelper.getKeyValuePair(config.REDISKEYS.TICS_BLOCKS, config.REDISKEYS.OLD_BLOCK_TICS)
            let new_block: any = await ticsProcessHelper.getKeyValuePair(config.REDISKEYS.TICS_BLOCKS, config.REDISKEYS.NEW_BLOCK_TICS)
            if (!old_block) {
                console.log("No block present in old block")
                await ticsProcessHelper.setBlocks(config.REDISKEYS.TICS_BLOCKS, config.REDISKEYS.OLD_BLOCK_TICS, this.minBlockConfirmation, null)
            }
            if (!new_block) {
                console.log("No block present in new block")
                await ticsProcessHelper.setBlocks(config.REDISKEYS.TICS_BLOCKS, config.REDISKEYS.NEW_BLOCK_TICS, this.minBlockConfirmation, null)
            }
            if (old_block && new_block) {
                let oldBlock: number = Number(old_block);
                let newBlock = Number(new_block);
                if (newBlock <= oldBlock) {
                    await ticsProcessHelper.setBlocks(config.REDISKEYS.TICS_BLOCKS, config.REDISKEYS.NEW_BLOCK_TICS, this.minBlockConfirmation, newBlock)
                } else {
                    let block_diff: number = newBlock - oldBlock;
                    if (block_diff > 50) {
                        await ticsProcessHelper.diff_blocks_add_in_db(newBlock, oldBlock)
                        await ticsProcessHelper.setKeyValuePair(config.REDISKEYS.TICS_BLOCKS, config.REDISKEYS.OLD_BLOCK_TICS, newBlock.toString())
                        await ticsProcessHelper.setBlocks(config.REDISKEYS.TICS_BLOCKS, config.REDISKEYS.NEW_BLOCK_TICS, this.minBlockConfirmation, newBlock)
                    } else {
                        await this.readBlocks(oldBlock, newBlock)
                    }
                }
            } else {
                console.log(" no old block and no new block")
                await global_helper.save_error_logs('Tics_setBlocks', 'No old block no new block')
            }
        } catch (err: any) {
            console.error("Error in getBlocks", err)
            await global_helper.save_error_logs('Tics_getBlocks', err.message)
        }
    }
    public async readBlocks(blockNumber: number, toBlockNumber: number) {
        try {
            let block_updated: number = blockNumber + 1;
            await ticsProcessHelper.setKeyValuePair(config.REDISKEYS.TICS_BLOCKS, config.REDISKEYS.OLD_BLOCK_TICS, block_updated.toString());
            console.table({
                LATEST_BLOCK: blockNumber,
                BLOCK_DIFFERENCE: toBlockNumber - blockNumber,
                TO_BLOCK_NUMBER: toBlockNumber
            });
            await this.getTxFromBlock(blockNumber, KEYS.latestBlock);
        } catch (err: any) {
            console.error("Error in readBlocks>>.", (err as Error).message)
            await global_helper.save_error_logs('TICS_readBlocks', err.message)
        }
    }
    public async getTxFromBlock(blockId: number, key: string) {
        const { getBlock } = Blockchain_Helper.Web3.eth;
        try {
            const blockInfo = await getBlock(blockId, true);
            if (blockInfo) {
                if (blockInfo.transactions) {
                    for await (let tx of blockInfo.transactions) {
                        let data: blocks_trnx = {
                            block_number: blockId,
                            tx_id: tx.hash ? tx.hash : '',
                            value: tx.value ? tx.value : '',
                            fromAddress: tx.from ? tx.from : '',
                            toAddress: tx.to ? tx.to : '',
                            retry_count: 0,
                            key: null,
                            pair: null
                        }
                        if (key == KEYS.latestBlock) {
                            data.key = 'latest';
                            // let queue_length: any = await rabbitMq.queue_length(config.TRANSACTIONS.eth_all_blocks)
                            // console.log("queue_length>>>", queue_length)
                            await kafkaHelper.produceMessage(this.tics_block_tx_topic, data);
                            //rabbitMq.send_tx_to_queue(config.TRANSACTIONS.eth_all_blocks, Buffer.from(JSON.stringify(data)))
                        } else {
                            data.key = 'behined & specific'
                            await kafkaHelper.produceMessage(this.tics_behind_block_tx_topic, data);
                            //rabbitMq.send_tx_to_queue(config.KAFKA_BLOCKS.eth_behined_block_topic, Buffer.from(JSON.stringify(data)))
                        }
                    }
                } else {
                    console.log("no block info trnasactions")
                }
            } else {
                console.log("No blockinfo");
                await TicsOldBLockModel.create({ block_number: blockId, coin_family: config.STATIC_COIN_FAMILY.BNB, start_block: blockId, end_block: blockId, status: 0 })
            }
        } catch (err: any) {
            console.error("Error in getTxFromBlock:", (err as Error).message);
            await global_helper.save_error_logs('Tics_readBehindBlock', err.message)
        }
    }
    public async readBehindBlock() {
        try {
            // REDIS BASED
            let data_exist: any = await ticsProcessHelper.getKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_TICS)
            let data_exists = JSON.parse(data_exist)
            if (data_exists) {
                console.log("This block exist in redis read behined")
                let id = data_exists.id
                let end_block = data_exists.end_block
                let start_block = data_exists.start_block

                if (end_block > start_block) {
                    console.log("This block go for getTxFromBlock")
                    await this.getTxFromBlock(start_block, KEYS.behinedBlock);
                    start_block = start_block + 1;
                    let add_data = {
                        id: id,
                        start_block: start_block,
                        end_block: end_block,
                        diff: end_block - start_block
                    }
                    let dataaaaa_exist: any = await ticsProcessHelper.setKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_TICS, JSON.stringify(add_data))
                } else {
                    console.log("id>>>>>>>>>>>", id)
                    // await EthOldBLockModel.destroy({ where: { id: id } })
                    await TicsOldBLockModel.update({ status: 1 }, { where: { id: id } })
                    await ticsProcessHelper.deleteKeyValuePair(config.BLOCKS, config.READ_BEHINED_BLOCK_TICS)

                    let behindBlocks: any = await ticsProcessHelper.behindBlocks()

                }
            } else {
                // DB BASED
                let behindBlocks: any = await ticsProcessHelper.behindBlocks()
            }

            // REDIS BASED
            // let data: any = await ticsProcessHelper.getHashTable(config.READ_BEHINED_BLOCK_ETH)
            // console.log("data?>>??", data.length)
            // if (data.length > 0) {
            //     for (let i: number = 0; i < data.length; i++) {
            //         await eth_blocks_process.getTxFromBlock(Number(data[i]), KEYS.behinedBlock);
            //         await ticsProcessHelper.deleteKeyValuePair(config.READ_BEHINED_BLOCK_ETH, data[i].toString())
            //     }
            // }
        } catch (err: any) {
            console.error("Error in readBehindBlock>>", err)
            await global_helper.save_error_logs('TICS_readBehindBlock', err.message)
        }
    }
    public async readSpecificBlock() {
        try {
            console.log("Entere into readSpecificBlock>>>")
            let specificBlocks: any = await ticsProcessHelper.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_TICS)
            let specific_block: any = JSON.parse(specificBlocks);
            if (specific_block && Number(specific_block) > 0) {
                console.table({ SPECIFIC_BLOCK: specific_block })
                await this.getTxFromBlock(specific_block, KEYS.readSpecificBlock)
                await ticsProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_TICS, '0')
            } else {
                console.log("Specific block is not present>>>>")
                await ticsProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_TICS, '0')
            }
        } catch (error: any) {
            console.error("Error in readSpecificBlock>>>", error)
        }
    }
    public async testingNode() {
        try {
            // Get Block Number
            let { getBlockNumber } = Blockchain_Helper.Web3.eth;
            let latestBlock = await getBlockNumber();
            console.log("Block  number>>", latestBlock)

            // Get Block Info
            const { getBlock } = Blockchain_Helper.Web3.eth;
            const blockInfo = await getBlock(latestBlock, true);
            console.log("blockInfo>>", blockInfo)

            // Get Block Receipt
            const { getTransactionReceipt } = Blockchain_Helper.Web3.eth;
            const txReceipt = await getTransactionReceipt(blockInfo.transactions[0].hash);
            console.log("txreceipt>>", txReceipt)

            // Get more details which was reduced
            const { getTransaction } = Blockchain_Helper.Web3.eth;
            const transaction = await getTransaction(blockInfo.transactions[0].hash);
            console.log("transaction>>", transaction)

        } catch (err: any) {
            console.error("Error in testingNode>>", err)
        }
    }
}

export const tics_blocks_process = new Tics_block_process();