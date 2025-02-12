export interface CoinPriceInFiatInterface {
    id: number;
    coin_id: number;
    coin_symbol: string;
    coin_name?: string;
    coin_family?: number;
    cmc_id?: number;
    fiat_type: string;
    value: number | 0;
    price_change_24h?: number | 0;
    price_change_percentage_24h?: number | 0;
    market_cap?: number;
    circulating?: number;
    total_supply?: number;
    rank?: number;
    volume_24h?: number;
    token_address?: string;
    max_supply?: number;
    latest_price: string | null;
    // roi?: number;
    // open?: number;
    // high?: number;
    // average?: number;
    // close?: number;
    // low?: number;
    // change_price?: number;
}
