import { CoinInterface } from "../models";

export interface blocksTrnx {
    block_number: number,
    tx_id: string | '',
    value: string | '',
    fromAddress: string | '',
    toAddress: string | '',
    retry_count: number,
    key: string | null,
    pair: string | null
}

export interface pendingTrnx {
    tx_id: string;
    coin_id: number,
    is_token: boolean,
    token_type: string | null,
    token_address: string
}

export interface balanceUpdateQueue {
    coin_data: any,
    wallet_address: string,
    queue_count: number
}

// Interface for the coin data within trx_data - extending CoinInterface
interface TransactionCoinData extends CoinInterface {
    // Additional properties specific to transaction processing
    token_address: string; // Override to make it required
}

// Interface for the main trx_data object
export interface TransactionData {
    coin_id: number;
    status: string;
    txid: string;
    gas_reverted: number | undefined;
    tx_fee: number;
    fromAddress: string;
    coin: TransactionCoinData;
}

// Interface for the txData parameter in sendTxUpdate
export interface TxUpdateData {
    coin: {
        coin_id: number;
        is_token: boolean;
        token_type: string | null;
        token_address: string;
    };
    status: string;
    txid: string;
    gasReverted: number | undefined;
    fromAddress: string;
    tx_fee: number;
}