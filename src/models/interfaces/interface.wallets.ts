export interface WalletInterface {
  wallet_id: number;
  user_id: number;
  // email?: string,
  wallet_name: string;
  wallet_address: string;
  coin_id: number;
  balance: number | string;
  // balance_blocked?: number | 0;
  // user_withdraw_limit?: number | 0;
  default_wallet?: number | 0;
  status?: number | 0;
  // is_verified?: number | 1;
  // is_deleted?: number | 0;
  sort_order?: number | null;
  is_private_wallet?: number | 0;
  // created_at: string;
  // updated_at: string;
  coin_family?: number;
}
