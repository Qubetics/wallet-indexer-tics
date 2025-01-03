export interface WalletInterface {
  wallet_id: number;
  user_id: number;
  // email?: string,
  wallet_name: string;
  wallet_address: string;
  coin_id: number;
  coin_name?: string;
  balance: number | string;
  balance_in_usd?: number;
  coin_fiat_price?: object;
  coin_image?: string | null | undefined;
  balance_blocked?: number | 0;
  user_withdraw_limit?: number | 0;
  default_wallet?: number | 0;
  status?: number | 0;
  is_verified?: number | 1;
  is_deleted?: number | 0;
  sort_order?: number | null;
  is_private_wallet?: number | 0;
  // created_at: string;
  // updated_at: string;
  coin_family?: number;
  coin_symbol?: string;
  is_token?: number;
  token_address?: string | null;
  decimals?: number | 0;
}
