import { config } from "../config/config";
import kafkaHelper from "../config/kafka.helper";
import { Blockchain_Helper, Utility_Helper } from "../helpers";
import { blocks_trnx } from "../interfaces/global_helper";
import ticsProcessHelper from "./process.helper";
import { TxType } from "../types/ERC20Tokens.types";
import { TrnxTypeEnum } from "../enum";
import { global_helper } from "../helpers/global_helper";
import { ethers, TransactionReceipt } from "ethers";

class queue_transactions {
    public approval: string | null;
    public swap: string | null;
    public transfer: string | null;
    public tics_transfer: string | null;
    public tics_block_tx_topic = config.KAFKA_BLOCKS.TICS_BLOCK_TX_TOPIC;
    public tics_block_tx_group_id = config.KAFKA_BLOCKS.TICS_BLOCK_TX_GROUP_ID;
    public tics_behind_block_tx_topic = config.KAFKA_BLOCKS.TICS_BEHIND_BLOCK_TX_TOPIC;
    public tics_behind_block_tx_group_id = config.KAFKA_BLOCKS.TICS_BEHIND_BLOCK_TX_GROUP_ID;
    public deposit_withdraw_topic = config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_TOPIC;

    constructor() {
        this.approval = ethers.id('Approval(address,address,uint256)');
        this.swap = ethers.id('Swap(address,uint256,uint256,uint256,uint256,address)');
        this.transfer = ethers.id('Transfer(address,address,uint256)');
        this.tics_transfer = ethers.id('Transfer(address,uint256)');

    }

    public startBlockTxQueue = async () => {
        let consumer = await kafkaHelper.kafka.consumer({ groupId: this.tics_block_tx_group_id });
        await consumer.subscribe({ topic: this.tics_block_tx_topic, fromBeginning: true });

        await consumer.run({
            eachMessage: ({ topic, partition, message }: { topic: any, partition: any, message: any }) => {
                this.read_trnx(JSON.parse(message.value.toString()))
            }
        }).catch((err: Error) => {
            console.log("🔥 ~ startBlockTxQueue consumer error", err);
        });
    };

    public startBehindBlockTxQueue = async () => {
        let consumer = await kafkaHelper.kafka.consumer({ groupId: this.tics_behind_block_tx_group_id });
        await consumer.subscribe({ topic: this.tics_behind_block_tx_topic, fromBeginning: true });

        await consumer.run({
            eachMessage: ({ topic, partition, message }: { topic: any, partition: any, message: any }) => {
                this.read_trnx(JSON.parse(message.value.toString()))
            }
        }).catch((err: Error) => {
            console.log("🔥 ~ startBehindBlockTxQueue consumer error", err);
        });
    };

    public async read_trnx(data: blocks_trnx) {
        try {
            const txReceipt: TransactionReceipt | null = await Blockchain_Helper.provider.getTransactionReceipt(data.tx_id);

            if (txReceipt && txReceipt == null && Number(data.retry_count) < 6) {
                data.retry_count = Number(data.retry_count) + 1;
                await kafkaHelper.produceMessage(this.tics_behind_block_tx_topic, data)
                return false;
            }

            if (txReceipt?.status === 0 && txReceipt !== null) {
                console.log("(txReceipt.status === false && txReceipt !== null")
            } else if (txReceipt) {
                await tics_tx_status_deposit_withdraw.buildTx(txReceipt, data)
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
    public async buildTx(txReceipt: TransactionReceipt, data: blocks_trnx) {
        try {
            if (txReceipt.status !== 0 && txReceipt.logs.length > 0) {
                const gasUsed = txReceipt.gasUsed ? txReceipt.gasUsed : 0;
                const gasPrice: any = txReceipt?.gasPrice ? Number(txReceipt.gasPrice) : 0;
                const txFee = await Utility_Helper.bigNumberSafeMath(
                    gasUsed, '*', gasPrice
                )
                const gasInTics = await Blockchain_Helper.convertWeiToTics(txFee);
                const nftTokensAddress: any = [];
                for await (const log of txReceipt.logs) {

                    if (log?.topics && log.topics[2] !== undefined) {
                        if (log?.topics[0] == this.approval) {
                            continue;
                        } else if (log?.topics[0] == this.transfer) {
                            const isToken: any = await ticsProcessHelper.checkIfContract(log.address);
                            if (isToken) {
                                const decimalPrecision = isToken.decimals.toString().length - 1;
                                let finalAmount = await Utility_Helper.bigNumberSafeMath(log.data, '/', Math.pow(10, decimalPrecision))
                                finalAmount = await Utility_Helper.exponentialToDecimal(finalAmount)
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
                            } else if (isToken == null) {
                                const isNFTToken = await ticsProcessHelper.checkIfOurNFTContract(log.address);
                                if (isNFTToken) {
                                    const transferData: any = await this.getTransferEventDetail(
                                        isNFTToken.token_address,
                                        log?.blockNumber
                                    );
                                    if (nftTokensAddress.indexOf(isNFTToken.token_address) == -1 && transferData?.length > 0) {
                                        const gasUsed = txReceipt.gasUsed ? Number(txReceipt.gasUsed) : 0;
                                        const hexToNumberGasPrice: any = txReceipt?.gasPrice ? Number(txReceipt.gasPrice) : 0;
                                        const txFee = await Utility_Helper.bigNumberSafeMath(gasUsed, '*', hexToNumberGasPrice);

                                        const gasInEth = await Blockchain_Helper.convertWeiToTics(txFee);

                                        nftTokensAddress.push(isNFTToken.token_address);
                                        const fromAddress = transferData[0]?.args[0];
                                        const toAddress = transferData[0]?.args[1];
                                        const tokenId = ethers.toNumber(transferData[0]?.args[2]);
                                        const txData: TxType = {
                                            txId: transferData[0].transactionHash,
                                            fromAddress: fromAddress,
                                            toAddress: toAddress,
                                            token: isNFTToken,
                                            amount: tokenId,
                                            blockId: log?.blockNumber,
                                            isNFTToken: true,
                                            txType: null,
                                            trnx_fee: Number(gasInEth)
                                        };
                                        await this.checkAddressInDB(txData);
                                    } else {
                                        return "Not get transfer data.";
                                    }

                                } else if (isNFTToken == null) {
                                    await tics_tx_status_deposit_withdraw.check_tics_deposit(data, txReceipt)
                                }
                            }
                        }
                    }else if (log?.topics && log.topics[1] !== undefined  && log.topics[0] !== undefined && log.topics[0] == this.tics_transfer) {
                                
                                const decimalPrecision = 18;

                                let finalAmount = await Utility_Helper.bigNumberSafeMath(log.data, '/', Math.pow(10, decimalPrecision))
                               
                                finalAmount = await Utility_Helper.exponentialToDecimal(finalAmount)
                              
                                const toAddress: string = '0x' + log.topics[1].substring(26, log.topics[1].length);
                                /* in case of swap trnx */
                                const fromAddress: string = log.address;
                                const txData: TxType = {
                                    txId: log.transactionHash,
                                    fromAddress: fromAddress,
                                    toAddress: toAddress,
                                    token: null,
                                    amount: finalAmount,
                                    blockId: log.blockNumber,
                                    isNFTToken: false,
                                    txType: null,
                                    trnx_fee: Number(gasInTics)
                                };
                                await this.checkAddressInDB(txData);
                          
                    }
                }
            } else {
                await tics_tx_status_deposit_withdraw.check_tics_deposit(data, txReceipt);
            }
        } catch (error: any) {
            console.error("buildTx error >>>", (error as Error).message);
            await global_helper.save_error_logs('Tics_buildTx', error.message)
        }
    };

    public async check_tics_deposit(data: blocks_trnx, txReceipt: TransactionReceipt) {
        try {
            /** for check the eth deposit */
            if (
                data.value !== '0' &&
                data.toAddress &&
                data.block_number
            ) {
                const gasUsed = txReceipt.gasUsed ? txReceipt.gasUsed : 0;
                const gasPrice: number = Number(txReceipt.gasPrice) || 0;
                const txFee = await Utility_Helper.bigNumberSafeMath(
                    gasUsed, '*', gasPrice
                )
                const gasInTics = await Blockchain_Helper.convertWeiToTics(txFee);
                const ethValue = await Blockchain_Helper.convertWeiToTics((data.value ? data.value : 0));
                const amount = parseFloat(ethValue.toString());
                const txData: TxType = {
                    txId: data.tx_id,
                    fromAddress: data.fromAddress.toLowerCase(),
                    toAddress: data.toAddress.toLowerCase(),
                    token: null,
                    amount,
                    blockId: data?.block_number,
                    isNFTToken: false,
                    txType: TrnxTypeEnum.NULL,
                    trnx_fee: Number(gasInTics)
                };
                await this.checkAddressInDB(txData);
            }
        } catch (error: any) {
            console.error("💥 ~ ~ check_tics_deposit error", (error as Error).message);
            await global_helper.save_error_logs('TICS_check_deposit', error.message)

        }
    }

    public async checkAddressInDB(txData: TxType) {
        try {
            let ourTrnx: any = null;
            let withdraw = await ticsProcessHelper.check_our_wallet_address(txData.fromAddress);
            if (withdraw) {
                console.log("Found the user in FROM (WITHDRAW) tx 😸");
                txData.txType = TrnxTypeEnum.WITHDRAW;
                ourTrnx = txData;
            }
            let deposit = await ticsProcessHelper.check_our_wallet_address(txData.toAddress);
            if (deposit) {
                console.log("Found the user in to (DEPOSIT) tx 😸");
                txData.txType = TrnxTypeEnum.DEPOSIT;
                ourTrnx = txData;
            }
            if (ourTrnx) {
                await tics_tx_status_deposit_withdraw.addTxToQueue(
                    ourTrnx,
                    this.deposit_withdraw_topic || ""
                );
            }
        } catch (err: any) {
            console.error("Error in checkAddressInDB>>", err)
        }
    }

    public getTransferEventDetail = async (
        tokenAddress: string,
        blockId: number
    ) => {
        try {
            const contract = new ethers.Contract(
                tokenAddress,
                config.ERC721ABI,
                Blockchain_Helper.provider
            );

            const filter = contract.filters.Transfer();
            const events = await contract.queryFilter(
                filter,
                blockId,
                blockId
            );

            return events;
        } catch (error) {
            console.log("🔥 ~ getTransferEventDetail error", (error as Error).message);
            return [];
        }
    };
    public async addTxToQueue(data: TxType, queueName: string) {
        try {
            await kafkaHelper.produceMessage(queueName, data)
        } catch (err: any) {
            console.error("addTxToQueue", (err as Error).message);
            await global_helper.save_error_logs('TICS_checkAddressInDB', err.message)

        }
    };

}
export const tics_tx_status_deposit_withdraw = new queue_transactions()
