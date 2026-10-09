import Web3 from 'web3';
import { BooleanEnum, TokenStandard } from "../enum";
import { Utility_Helper } from "./utility.helper";
import { config } from '../config';
import { AbiItem } from 'web3-utils';

class BlockchainHelper {
    public Web3: Web3;
    public FULLNODE: string = config.config.NODE.TICS_RPC_URL;

    constructor() {
        const provider = new Web3.providers.HttpProvider(this.FULLNODE);
        this.Web3 = new Web3(provider);
    }

    //TICS
    convertWeiToTics = async (
        value: number
    ) => {
        try {
            return this.Web3.utils.fromWei(
                value.toString(),
                'ether'
            );
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
            let balance: number = 0;
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
            return balance
        } catch (error: any) {
            console.error(`Bsc_Helper BSC_Fetch_Balance error >>`, error);
            return null;
        }
    }

    public ERC20_Token_Fetch_Balance = async (
        wallet_address: string,
        contract_address: string | null
    ) => {
        try {
            if (contract_address) {
                const contract = new this.Web3.eth.Contract(
                    config.config.CONTRACT_ABI as AbiItem[],
                    contract_address
                );
                const decimals = await contract.methods.decimals().call();
                const balance = await contract.methods.balanceOf(wallet_address).call();
                const tokenBalance = await Utility_Helper.bigNumberSafeMath(
                    balance, '/', Math.pow(10, decimals)
                );
                let tokBalance: string = await Utility_Helper.exponentialToDecimal(tokenBalance)
                return Number(tokBalance);
            }
            return 0;
        } catch (error) {
            console.error(`BscHelper BEP20_Token_Fetch_Balance error >>`, error);

            throw error;
        }
    }

    private ERC721_Token_Fetch_Balance = async (
        wallet_address: string,
        contract_address: string | null
    ) => {
        try {
            if (contract_address) {
                const contract = await new this.Web3.eth.Contract(
                    config.config.ERC721ABI as AbiItem[],
                    contract_address
                );
                const balance = await contract.methods.balanceOf(wallet_address).call();
                return Number(balance);
            }
            return 0;
        } catch (error) {
            console.error(`Bsc_Helper BEP20_Token_Fetch_Balance error >>`, error);
            throw error;
        }
    }

    public Coin_Fetch_Balance = async (
        address: string
    ) => {
        try {
            const balance: string = await this.Web3.eth.getBalance(address);
            const tokenBalance = await Utility_Helper.bigNumberSafeMath(
                balance, '/', Math.pow(10, 18)
            );
            let coinBalance: string = await Utility_Helper.exponentialToDecimal(tokenBalance)
            return Number(coinBalance || 0);
        } catch (error: any) {
            console.error(`TICS_Helper Coin_Fetch_Balance error >>`, error);

            throw error;
        }
    }


}
export let Blockchain_Helper = new BlockchainHelper();