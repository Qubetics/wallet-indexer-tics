export interface NotificationInterface {
  title: string;
  notification_id?: number;
  message: string;
  amount: any;
  coin_symbol: string | null;
  coin_id: number;
  coin_family: number;
  from_user_id: number;
  to_user_id?: number | null;
  wallet_address?: string | null;
  notification_type: string;
  tx_id?: number | null;
  tx_type?: string;
  // resent_count?: number | null;
  view_status?: string | null;
  state: string | null;
  // cmc_id?: number;
  // coin_price_in_usd?: {}
}
