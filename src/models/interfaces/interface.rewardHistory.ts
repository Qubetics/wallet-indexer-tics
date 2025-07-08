export interface IRewardHistoryInterface {
    id: number,
    user_id: number,
    address: string | null;
    currency: string;
    amount: string | null;
    percentage: string | null;
    tx_id: number | null;
    status: string;
    type: string
}
