import { config } from '../config/config';
import axios, { AxiosInstance } from "axios";

//class UTXO {
class UTXO {

    // private btc_client_url = config.NODE.BTC_RPC_URL;
    // private ltc_client_url = config.LTC_RPC_URL;

    // // private clineAPIKEY = config.BTC_NODE_API_KEY;
    // private btc_axios_client: AxiosInstance;
    // private ltc_axios_client: AxiosInstance;
    public config: any;


    // constructor() {
    //     this.config = {
    //         method: 'get',
    //         headers: {
    //             'apikey': `${config.NODE.BTC_API_KEY}`,
    //             'Content-Type': 'application/json',
    //         }
    //     };
    //     // this.btc_axios_client = axios.create({
    //     //     baseURL: this.btc_client_url,
    //     //     headers: {
    //     //         // "api-key": this.clineAPIKEY,
    //     //         "Content-Type": "application/json",
    //     //     },
    //     // });
    //     // this.ltc_axios_client = axios.create({
    //     //     baseURL: this.ltc_client_url,
    //     //     headers: {
    //     //         // "api-key": this.clineAPIKEY,
    //     //         "Content-Type": "application/json",
    //     //     },
    //     // });
    // }


    // async getUserBtcBalance(address: string) {
    //     try {
    //         this.config.url = `${config.NODE.BTC_RPC_URL}api/v2/address/${address}`
    //         const response = await axios(this.config);
    //         return response.data.balance / 100000000;
    //     } catch (error: any) {
    //         console.error('error getUserBtcBalance >>>>>>', error);
    //         // return (error as Error).message;
    //         // return (error as Error).message;
    //         return null;
    //     }
    // }


    // async getUserLtcBalance(address: string) {
    //     try {
    //         const response = await this.ltc_axios_client.get(`api/v2/address/${address}`);
    //         return response.data.balance / 100000000;
    //     } catch (error:any) {
    //         console.error('getUserLtcBalance >>>>>>', error);
    //         return (error as Error).message;
    //     }
    // }


}

var utxo = new UTXO();
export default utxo;
