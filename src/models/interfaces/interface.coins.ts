export interface CoinInterface {
    coin_id: number;
    coin_name: string;
    coin_symbol: string;
    coin_image?: string | null;
    coin_family: number;
    cmc_id?: number;
    is_on_cmc?: number;
    coin_status: number | 0;
    is_token: number | 0;
    token_type: string | null;
    decimals: number | 0;
    token_abi?: string | null;
    token_address?: string | null;
    created_at?: string;
    updated_at?: string;
    for_swap?: number;
    added_by?: string;
}
