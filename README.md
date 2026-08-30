# Chinook Dimensional Model — Documentation

---

## Objective

Convert the normalized Chinook dataset into a dimensional model built for reads, deployed as a Raw → Clean → Mart
pipeline in Databricks, and use it to answer six business questions.


<!-- ─────────── new %md cell ─────────── -->

## 1. Data flow


```

  11 CSV files (Unity Catalog Volume)
        │
        ▼
  ┌───────────────┐
  │  Week5.raw    │  landed as-is, explicit schema, no transformation
  └───────┬───────┘
          ▼
  ┌────────────────────────┐
  │  Pre-cleaning analysis │  key uniqueness · completeness ·
  └───────┬────────────────┘  distributions · referential integrity
          │
          └──► fed back into ingestion (Track.csv reloaded with escape => '"')
          ▼
  ┌───────────────┐
  │  Week5.clean  │  cast · trim · null-standardize · DQ flags · Validations
  └───────┬───────┘  11 tables, 1:1 with raw
          ▼
  ┌───────────────┐
  │  Week5.mart   │  star schema — 4 dims, 1 fact, 1 aggregate
  └───────┬───────┘
          ▼
     6 views  →  Dashboard

```

| Layer | Purpose | Objects |
|---|---|---|
| `Week5.raw` | source fidelity — CSVs as loaded | 11 tables |
| `Week5.clean` | typed, standardized, DQ-tagged | 11 tables |
| `Week5.mart` | star schema for analysis | 4 dims + 1 fact + 1 aggregate |
| views | one per business question | 6 views |

<!-- ─────────── new %md cell ─────────── -->
<!--Shiena: ##2. How to Run -->



<!-- ─────────── new %md cell ─────────── -->
<!--MJ: ## 3. Modelling journey -->


<!-- ─────────── new %md cell ─────────── -->
<!--Kinah:## 4. Data quality framework -->


<!-- ─────────── new %md cell ─────────── -->
<!-- Vee:## 5. Validation-->

