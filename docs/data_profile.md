# Data profile – EEA Waterbase Water Quality (AggregatedData)

## Source
- **Dataset:** EEA "Waterbase – Water Quality ICM", 2026 release, file `WISE6_AggregatedData-csv.zip`
- **Full file:** `Waterbase_v2025_1_T_WISE6_AggregatedData.csv`, 2.1 GB, 6,450,192 rows, 32 columns, years 1931–2024
- **Grain:** one row = annual statistics (min, mean, max, median) for one determinand at one monitoring site in one year

## Project subset
Created by `scripts/make_subset.py`.

| Property | Value |
|---|---|
| Rows | 1,173,094 |
| Columns | 32 (all kept) |
| Years | 2010–2024 |
| Countries | 36 |
| Determinands | 20 (the most frequently measured) |
| Primary key | `UID` (unique, never null) |

**Why this subset:** 6.45M rows is heavy for a free Azure SQL database. The 20 most-measured determinands over 15 years keep the analysis meaningful and the size close to the original course dataset (just over 1M rows).

**Determinands kept:**
- **Nutrients:** Ammonium, Nitrate, Nitrite, Phosphate, Total phosphorus, Total nitrogen
- **Oxygen and physical:** Dissolved oxygen, Oxygen saturation, BOD5, pH, Water temperature, Electrical conductivity
- **Metals:** Lead, Cadmium, Nickel, Copper, Zinc, Arsenic, Mercury
- **Pesticides:** Atrazine

## Key columns
| Column | Meaning |
|---|---|
| `countryCode` | Two-letter country code (e.g. ES, FR) |
| `monitoringSiteIdentifier` | Monitoring site ID |
| `parameterWaterBodyCategory` | RW = river, GW = groundwater, LW = lake, TW = transitional, CW = coastal |
| `observedPropertyDeterminandLabel` | Substance or property measured |
| `resultUom` | Unit of measure |
| `phenomenonTimeReferenceYear` | Year of the measurements |
| `resultNumberOfSamples` | Number of samples behind the statistics |
| `resultMinimumValue`, `resultMeanValue`, `resultMaximumValue`, `resultMedianValue` | Annual statistics |
| `result*BelowLOQ` | Flags showing a value was below the limit of quantification (LOQ) |
| `metadata_statements` | EEA quality-check messages |

Check `WISE6_dataset_definition.zip` for exact code meanings before using the status columns.

## Data quality issues (inputs for the silver layer)
| Issue | Rows | Suggested handling |
|---|---|---|
| `resultMeanValue` is null | 13,879 | Drop, since the mean is the main measure |
| Minimum greater than maximum | 353 | Quarantine or drop |
| Sample count null or ≤ 0 | 432 | Drop |
| EEA quality-check messages present | 212,087 | Keep, but add a `has_qc_warning` flag |
| Phosphate in `mg{PO4}/L` instead of `mg{P}/L` | 930 | Convert to `mg{P}/L`: multiply by 30.97 / 94.97 ≈ 0.3261 |
| Dissolved oxygen in `mg{O2}/L` and `mg/L` | 410 vs 53,084 | Same unit, written two ways; standardize to one label |
| Oxygen saturation in `%{oxygenSaturation}` and `%` | 17 vs 17,365 | Standardize to `%` |
| Spain is 53% of all rows | 617,581 | No fix needed, but mention it in the dashboard |
| Negative water temperatures | possible | Valid (below-zero readings); don't treat as errors |

## Suggested gold tables
- Yearly average per country and determinand (trend lines)
- Latest-year snapshot per country (map or bar chart)
- Nutrient levels by water body category
- Share of sites exceeding a threshold (e.g. nitrate above 50 mg/L, the EU drinking water limit)
