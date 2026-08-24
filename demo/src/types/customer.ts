export const customerStatuses = ["active", "inactive", "suspended"] as const;
export const customerRiskRatings = ["low", "standard", "high"] as const;
export const customerAddressTypes = ["residential", "mailing", "work"] as const;
export const customerContactTypes = ["email", "mobile_phone", "home_phone"] as const;

export type CustomerStatus = (typeof customerStatuses)[number];
export type CustomerRiskRating = (typeof customerRiskRatings)[number];
export type CustomerAddressType = (typeof customerAddressTypes)[number];
export type CustomerContactType = (typeof customerContactTypes)[number];

export type CustomerProfileInput = {
  preferredLanguage: string;
  occupation: string | null;
  annualIncome: string | null;
  taxResidencyCountry: string | null;
  riskRating: CustomerRiskRating;
};

export type CustomerAddressInput = {
  type: CustomerAddressType;
  line1: string;
  line2: string | null;
  city: string;
  region: string | null;
  postalCode: string;
  countryCode: string;
  validFrom: string;
  validTo: string | null;
};

export type CustomerContactInput = {
  type: CustomerContactType;
  value: string;
  isPrimary: boolean;
  verifiedAt: string | null;
};

export type CustomerPreferencesInput = {
  marketingEmailOptIn: boolean;
  marketingSmsOptIn: boolean;
  paperlessStatements: boolean;
  notificationChannels: Record<string, boolean>;
};

export type CustomerInput = {
  customerNumber: string;
  firstName: string;
  lastName: string;
  dateOfBirth: string | null;
  status: CustomerStatus;
  profile: CustomerProfileInput;
  addresses: CustomerAddressInput[];
  contacts: CustomerContactInput[];
  preferences: CustomerPreferencesInput;
};

export type Customer = CustomerInput & {
  id: string;
  createdAt: string;
  updatedAt: string;
};

export type CustomerPage = { items: Customer[]; page: number; total: number; totalPages: number };
export type CustomerDashboard = { postgres: Customer[]; mongodb: Customer[]; pagination: { postgres: Omit<CustomerPage, "items">; mongodb: Omit<CustomerPage, "items"> } };
export type DatabaseEvent = { source: "postgres" | "mongodb" | "system"; operation: string; at: string; customerNumber?: string };
export type CustomerSimulationStatus = {
  running: boolean;
  intervalMs: number;
  created: number;
  startedAt: string | null;
  lastCreatedAt: string | null;
  lastError: string | null;
};
