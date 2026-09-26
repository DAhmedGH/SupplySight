# Demo outline

Target length: **3–5 minutes**. Use the fixed historical DEV scenario and genuine Power BI Desktop pages. Keep credentials, account identifiers, and local key paths off screen. Do not commit a video file.

1. **Problem (20–30 seconds):** A retailer needs consistent views of demand, stock, supplier service, and possible replenishment actions.
2. **Architecture (30 seconds):** Walk through the [architecture diagram](architecture.md), calling out Python source handling, dbt analytical definitions, and Power BI presentation.
3. **Source and warehouse (40–50 seconds):** Show the deterministic nine-file generator, RAW audit/quarantine behavior, CORE model, and one reconciliation test.
4. **Operation (20–30 seconds):** Show the manual Airflow DAGs and the automatic CI checks. State that the fixed historical scenario is rebuilt through the approved manual DEV path.
5. **Forecasting (30–40 seconds):** Explain the three-month mean versus damped Holt comparison, baseline selection, and 61.05% holdout WAPE.
6. **Inventory intelligence (30–40 seconds):** Explain rule-based tiers, modeled shortage, reorder suggestions, and the narrow transfer result. State the feasibility limits.
7. **Report (60–90 seconds):** Visit Executive Overview, Inventory & Replenishment, Supplier & Logistics, and Forecasting & Demand. Keep the 2024 cutoff and Jan–Mar 2025 horizon visible.
8. **Close (15–20 seconds):** Point to setup and tests, then name longer observed history and better uncertainty calibration as possible future work.
