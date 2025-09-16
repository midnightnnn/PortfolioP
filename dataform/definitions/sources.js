// definitions/sources/sources.js
const project = dataform.projectConfig.defaultDatabase;
const schema  = "portfolio";

[
  "assets",
  "account_balances",
  "orders",
  "daily_stock_prices",
  "dividend_history",
  "daily_exchange_rates",
  "asset_class",
  "manual_sheet",
  "target_allocations",
].forEach(name =>
  declare({
    database: project,
    schema: schema,
    name: name
  })
);