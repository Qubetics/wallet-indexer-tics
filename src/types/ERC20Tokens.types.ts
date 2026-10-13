import {
  BlockChainStatusEnum,
  TrnxTypeEnum
} from "../enum";
import { CoinInterface } from "../models";


type ERC20Tokens = {
  coin_symbol: string;
  token_address: string;
  decimals: number;
};


export type TxType = {
  txId: string;
  fromAddress: string;
  toAddress: string;
  token: ERC20Tokens | null;
  amount: number;
  blockId: number;
  isNFTToken?: boolean;
  txType: TrnxTypeEnum | null;
  trnx_fee: number
};

export type WebHookTxType = {
  tx_id: string;
  from_address: string;
  to_address: string;
  amount: number;
  block_id: number;
  coin: CoinInterface;
  isNFTToken: boolean;
  status: string;
  blockchain_status: BlockChainStatusEnum,
  txType: TrnxTypeEnum | null;
  trnx_fee: number
};
