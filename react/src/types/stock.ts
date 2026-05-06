export interface StockData {
  timestamp: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface SupportResistanceLevel {
  price: number;
  strength?: string;
  probability?: number;
  label?: string;
}

export interface AnalysisData {
  symbol: string;
  currentPrice: number;
  priceData: StockData[];
  supportLevels: SupportResistanceLevel[];
  resistanceLevels: SupportResistanceLevel[];
  summary: string;
  action: string;
  trendDirection: string;
  targets: Array<{ price: number; timeframe: string; probability: number }>;
}
