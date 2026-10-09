import { BooleanEnum, TokenStandard } from "../enum";
import { Utility_Helper } from "./utility.helper";
import { config } from '../config';
import { ethers } from 'ethers';

class BlockchainHelper {
    public provider: ethers.JsonRpcProvider;
  
    constructor() {
      this.provider = new ethers.JsonRpcProvider(config.config.NODE.TICS_RPC_URL || "");
      this.initialize();
    }
  
  
    public initialize() {}

    //TICS
    convertWeiToTics = async (
        value: number | string
    ) => {
        try {
            const valueAsString = value.toString();
            return ethers.formatEther(valueAsString);;
        } catch (error: any) {
            console.error(`ConvertWeiToTics error >>`, error);
            return 0;
        }
    }

    Fetch_Balance = async (
        address: string,
        coin: any
    ) => {
        try {
            let balance: number | null = null;
            if (coin.is_token == BooleanEnum.true) {
                switch (coin.token_type.toLowerCase()) {

                    case TokenStandard.ERC20:
                        balance = await this.ERC20_Token_Fetch_Balance(
                            address, //// wallet_address
                            coin.token_address ? coin.token_address : null/// contract_address
                        )
                        break;
                    default:
                        balance = 0
                        break;
                }
            } else {
                balance = await this.Coin_Fetch_Balance(address);
            }
            return balance;
        } catch (error: any) {
            console.error(`Tics_Helper Fetch_Balance error >>`, error);
            return null;
        }
    }

    public ERC20_Token_Fetch_Balance = async (
        wallet_address: string,
        contract_address: string | null
    ) => {
        try {
            if (contract_address) {
                const contract = new ethers.Contract(
                  contract_address,
                  config.config.CONTRACT_ABI,
                  this.provider
                );
                const decimals = Number(await contract.decimals())
                const balance = await contract.balanceOf(wallet_address);
                const tokenBalance = await Utility_Helper.bigNumberSafeMath(
                  balance.toString(),
                  "/",
                  Math.pow(10, decimals)
                );
                const tokBalance: string = await Utility_Helper.exponentialToDecimal(tokenBalance)
                return Number(tokBalance);
              }
              return null;
        } catch (error) {
            console.error(`Tics_Helper ERC20_Token_Fetch_Balance error >>`, error);
            return null;
        }
    }


    public Coin_Fetch_Balance = async (
        address: string
    ) => {
        try {
            const balance = await this.provider.getBalance(address);
            const tokenBalance = await Utility_Helper.bigNumberSafeMath(
                balance.toString(),
                "/",
                Math.pow(10, 18)
            );
            const coinBalance: string = await Utility_Helper.exponentialToDecimal(tokenBalance)
            return Number(coinBalance);
        } catch (error: any) {
            console.error(`TICS_Helper Coin_Fetch_Balance error >>`, error);
            return null;
        }
    }


}
export let Blockchain_Helper = new BlockchainHelper();