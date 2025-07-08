import { config } from "../config/config";
import kafkaHelper from "../config/kafka.helper";
import { blockChainHelper, utilityHelper } from "../helpers";
import { blocksTrnx } from "../interfaces/globalHelper";
import ticsProcessHelper from "./process.helper";
import { TxType } from "../types/ERC20Tokens.types";
import { TrnxTypeEnum } from "../enum";
import { globalHelper } from "../helpers/global.helper";
import { ethers, TransactionReceipt } from "ethers";

class ProcessedBlockTransactions {
    public approval: string | null;
    public swap: string | null;
    public transfer: string | null;

    public tics_block_tx_topic = config.KAFKA_BLOCKS.TICS_BLOCK_TX_TOPIC;
    public tics_block_tx_group_id = config.KAFKA_BLOCKS.TICS_BLOCK_TX_GROUP_ID;
    public tics_behind_block_tx_topic = config.KAFKA_BLOCKS.TICS_BEHIND_BLOCK_TX_TOPIC;
    public tics_behind_block_tx_group_id = config.KAFKA_BLOCKS.TICS_BEHIND_BLOCK_TX_GROUP_ID;
    public deposit_withdraw_topic = config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_TOPIC;

    constructor() {
        this.approval = ethers.id('Approval(address,address,uint256)');
        this.swap = ethers.id('Swap(address,uint256,uint256,uint256,uint256,address)');
        this.transfer = ethers.id('Transfer(address,address,uint256)');
    }

    public startBlockTxQueue = async () => {
        let consumer: any = null;
        let isConnected = false;

        const connectConsumer = async () => {
            try {
                if (consumer) {
                    await consumer.disconnect();
                }

                consumer = await kafkaHelper.kafka.consumer({
                    groupId: this.tics_block_tx_group_id,
                    // Add session timeout for better error handling
                    sessionTimeout: 30000,
                    // Add heartbeat interval
                    heartbeatInterval: 3000,
                });

                await consumer.subscribe({
                    topic: this.tics_block_tx_topic,
                    fromBeginning: false, // Changed to false to prevent reprocessing
                });

                await consumer.run({
                    // Add proper autoCommit configuration
                    autoCommit: true,
                    autoCommitInterval: 5000,
                    autoCommitThreshold: 100,

                    eachMessage: async ({ topic, partition, message }: { topic: any; partition: any; message: any }) => {
                        try {
                            console.log("🚀 ~ startBlockTxQueue topic >>>>", topic);
                            console.log("🚀 ~ startBlockTxQueue message.value >>>>", message.value.toString());
                            await this.readTrnx(JSON.parse(message.value.toString()));
                        } catch (error) {
                            console.error(`❌ Error processing message:`, error);
                        }
                    },
                });
                isConnected = true;
            } catch (error) {
                console.error(`❌ Failed to connect consumer:`, error);
                isConnected = false;
                throw error;
            }
        };

        // Add reconnection logic
        const startWithReconnection = async () => {
            while (true) {
                try {
                    if (!isConnected) {
                        console.log(`🔄 Attempting to connect consumer startBlockTxQueue...`);
                        await connectConsumer();
                    }

                    // Keep the consumer alive
                    await new Promise(resolve => setTimeout(resolve, 30000));
                } catch (error) {
                    console.error(`❌ Consumer connection error:`, error);
                    isConnected = false;

                    // Wait before retrying
                    console.log(`⏳ Waiting 10 seconds before reconnecting...`);
                    await new Promise(resolve => setTimeout(resolve, 10000));
                }
            }
        };

        // Start the consumer with reconnection
        await startWithReconnection();
    };

    public startBehindBlockTxQueue = async () => {
        let consumer: any = null;
        let isConnected = false;

        const connectConsumer = async () => {
            try {
                if (consumer) {
                    await consumer.disconnect();
                }

                consumer = await kafkaHelper.kafka.consumer({
                    groupId: this.tics_behind_block_tx_group_id,
                    // Add session timeout for better error handling
                    sessionTimeout: 30000,
                    // Add heartbeat interval
                    heartbeatInterval: 3000,
                });

                await consumer.subscribe({
                    topic: this.tics_behind_block_tx_topic,
                    fromBeginning: false, // Changed to false to prevent reprocessing
                });

                await consumer.run({
                    // Add proper autoCommit configuration
                    autoCommit: true,
                    autoCommitInterval: 5000,
                    autoCommitThreshold: 100,

                    eachMessage: async ({ topic, partition, message }: { topic: any; partition: any; message: any }) => {
                        try {
                            console.log("🚀 ~ startBehindBlockTxQueue topic >>>>", topic);
                            console.log("🚀 ~ startBehindBlockTxQueue message.value >>>>", message.value.toString());
                            await this.readTrnx(JSON.parse(message.value.toString()));
                        } catch (error) {
                            console.error(`❌ Error processing message:`, error);
                        }
                    },
                });
                isConnected = true;
            } catch (error) {
                console.error(`❌ Failed to connect consumer:`, error);
                isConnected = false;
                throw error;
            }
        };

        // Add reconnection logic
        const startWithReconnection = async () => {
            while (true) {
                try {
                    if (!isConnected) {
                        console.log(`🔄 Attempting to connect consumer startBehindBlockTxQueue...`);
                        await connectConsumer();
                    }

                    // Keep the consumer alive
                    await new Promise(resolve => setTimeout(resolve, 30000));
                } catch (error) {
                    console.error(`❌ Consumer connection error:`, error);
                    isConnected = false;

                    // Wait before retrying
                    console.log(`⏳ Waiting 10 seconds before reconnecting...`);
                    await new Promise(resolve => setTimeout(resolve, 10000));
                }
            }
        };

        // Start the consumer with reconnection
        await startWithReconnection();
    };
    
    public async readTrnx(data: blocksTrnx) {
        try {
            const txReceipt: TransactionReceipt | null = await blockChainHelper.provider.getTransactionReceipt(data.tx_id);
            if (txReceipt == null && Number(data.retry_count) < 6) {
                data.retry_count = Number(data.retry_count) + 1;
                await kafkaHelper.produceMessage(this.tics_behind_block_tx_topic, data)
                return false;
            }

            if (txReceipt?.status == 0) {
                console.log("(txReceipt.status === false && txReceipt !== null")
            } else if (txReceipt) {
                await this.buildTx(txReceipt, data)
            }
        } catch (error: any) {
            console.error("Error in read_trnx>>>", error)
            if (Number(data.retry_count) < 3) {
                data.retry_count = Number(data.retry_count) + 1;
                console.log("Adding transaction in queue>>>", data.retry_count, "transaction id>>>", data)
                await kafkaHelper.produceMessage(this.tics_behind_block_tx_topic, data)
            }
        }
    }
    
    public async buildTx(txReceipt: TransactionReceipt, data: blocksTrnx) {
        try {
            if (txReceipt.status !== 0 && txReceipt.logs.length > 0) {
                for await (const log of txReceipt.logs) {
                    const isToken: any = await ticsProcessHelper.checkIfContract(log.address);
                    if (isToken && log?.topics && log?.topics[2] !== undefined && log?.topics[0] !== undefined) {
                        if (log.topics[0] == this.approval) {
                            continue;
                        } else if (log.topics[0] == this.transfer) {
                            const gasUsed = txReceipt.gasUsed ? Number(txReceipt.gasUsed) : 0;
                            const hexToNumberGasPrice: any = txReceipt?.gasPrice ? Number(txReceipt.gasPrice) : 0;
                            const txFee = await utilityHelper.bigNumberSafeMath(gasUsed, '*', hexToNumberGasPrice);

                            const gasInTics = await blockChainHelper.convertWeiToTics(txFee);

                            const decimalPrecision = isToken?.decimals?.toString().length - 1;
                            let finalAmount = await utilityHelper.bigNumberSafeMath(log.data, '/', Math.pow(10, decimalPrecision))
                            finalAmount = await utilityHelper.exponentialToDecimal(finalAmount)
                            let toAddress: string = '0x' + log.topics[2].substring(26, log.topics[2].length);
                            /* in case of swap trnx */
                            let fromAddress: string = '0x' + log.topics[1].substring(26, log.topics[1].length);
                            const txData: TxType = {
                                txId: log.transactionHash,
                                fromAddress: fromAddress,
                                toAddress: toAddress,
                                token: isToken,
                                amount: finalAmount,
                                blockId: log.blockNumber,
                                isNFTToken: false,
                                txType: null,
                                trnx_fee: Number(gasInTics)
                            };
                            await this.checkAddressInDB(txData);
                        }
                    } else if (isToken == null && log?.topics[2] !== undefined && log?.topics[0] == this.transfer) {
                        await this.checkTicsDeposit(data, txReceipt);
                    }
                    
                    /** swap transaction then also check eth */
                    if (log?.topics && log?.topics[0] == this.swap) {
                        await this.checkTicsDeposit(data, txReceipt);
                    }
                }
            } else {
                await this.checkTicsDeposit(data, txReceipt);
            }
        } catch (error: any) {
            console.error("buildTx error >>>", (error as Error).message);
            await globalHelper.saveErrorLogs('Tics_buildTx', error.message)
        }
    };
    
    public async checkTicsDeposit(data: blocksTrnx, txReceipt: TransactionReceipt) {
        try {
            /** for check the eth deposit */
            if (
                data.value !== '0' &&
                data.toAddress &&
                data.block_number
            ) {
                const gasUsed = txReceipt.gasUsed ? txReceipt.gasUsed : 0;
                const effectiveGasPrice: number = Number(txReceipt.gasPrice) || 0;
                const txFee = await utilityHelper.bigNumberSafeMath(gasUsed, '*', effectiveGasPrice);
                const gasInTics = await blockChainHelper.convertWeiToTics(txFee);

                const ticsValue = await blockChainHelper.convertWeiToTics((data.value ? data.value : 0));
                const amount = parseFloat(ticsValue.toString());

                const txData: TxType = {
                    txId: data.tx_id,
                    fromAddress: data.fromAddress.toLowerCase(),
                    toAddress: data.toAddress.toLowerCase(),
                    token: null,
                    amount,
                    blockId: data?.block_number,
                    isNFTToken: false,
                    txType: null,
                    trnx_fee: Number(gasInTics)
                };
                await this.checkAddressInDB(txData);
            }
        } catch (error: any) {
            console.error("💥 ~ ~ check_tics_deposit error", (error as Error).message);
            await globalHelper.saveErrorLogs('TICS_check_deposit', error.message)
        }
    }
    
    public async checkAddressInDB(txData: TxType) {
        try {
            const addresses = [txData.fromAddress, txData.toAddress].filter(Boolean);

            if (addresses.length > 0) {
                const results = await ticsProcessHelper.getMultipleKeyValuePairs(`${config.TICS_WALLET_ADDRESS}`, addresses);

                if (results) {
                    const [withdrawTx, depositTx] = results as string[];
                    if (depositTx) {
                        txData.txType = TrnxTypeEnum.DEPOSIT;
                    } else if (withdrawTx) {
                        txData.txType = TrnxTypeEnum.WITHDRAW;
                    }
                }
            }
            
            if (txData.txType !== null) {
                await this.addTxToQueue(
                    txData,
                    this.deposit_withdraw_topic || ""
                );
            }
        } catch (err: any) {
            console.error("Error in checkAddressInDB>>", err)
        }
    }

    public async addTxToQueue(data: TxType, queueName: string) {
        try {
            await kafkaHelper.produceMessage(queueName, data)
        } catch (err: any) {
            console.error("addTxToQueue", (err as Error).message);
            await globalHelper.saveErrorLogs('TICS_checkAddressInDB', err.message)
        }
    };
}

export const processedBlockTransactions = new ProcessedBlockTransactions()
