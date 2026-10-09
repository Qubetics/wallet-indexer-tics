export interface WalletInterface {
  wallet_id: number;
  user_id: number;
  // email?: string,
  wallet_name: string;
  wallet_address: string;
  coin_id: number;
  balance: number | string;
  default_wallet?: number | 0;
  status?: number | 0;
  sort_order?: number | null;
  is_private_wallet?: number | 0;
  coin_family?: number;
}
