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



<!-- ─────────── new %md cell ─────────── -->
## 6. Challenge 1 — the CSV parsing bug

### What we saw

The track price distribution returned four values that were not prices:

| UnitPrice | tracks |
|---|---|
| 5,760,129.00 | 1 |
| 4,195,542.00 | 1 |
| 274,504.00 | 1 |
| 142,081.00 | 1 |
| 1.99 | 213 |
| 0.99 | 3,284 |
| NULL | 2 |

### What we almost did

Treat them as statistical outliers and exclude them. That would have deleted
good data and left the underlying defect in place for the next load.

### Diagnosis

The magnitudes matched other columns — 5.7M and 4.2M are `Bytes`-scale;
274K and 142K are `Milliseconds`-scale. Inspecting the rows confirmed a
**field shift**:

```
Track 3412 as parsed:
  Name         "\"\"Eine Kleine Nachtmusik\"\" Serenade In G"
  AlbumId      NULL      ← should be 281
  MediaTypeId  281       ← actually AlbumId
  GenreId      2         ← actually MediaTypeId
  Composer     "24"      ← actually GenreId
  Bytes        348971    ← actually Milliseconds
  UnitPrice    5760129   ← actually Bytes
```

**Root cause:** the track name contains doubled quotes (`""`), the RFC 4180
way to escape a quote inside a quoted field. Spark's default CSV `escape`
character is a **backslash**, not a doubled quote. The parser split the field
and everything after it shifted one or two positions.

**Six rows** were affected, not four — four with wrong prices, two with NULLs.
`3,284 + 213 + 6 = 3,503`, the full track count.

### Fix

```sql
read_files(..., quote => '"', escape => '"', multiLine => true, ...)
```

### What we learned

Profile distributions before cleaning. The query ran without error and the data looked plausible at a glance — only looking at the shape of the values
exposed it. 


<!-- ─────────── new %md cell ─────────── -->

## 6. Challenge 2 — the dataset is synthetic

Several answers came back flat. We checked whether that was our analysis or the data, and found the same signature at four independent grains.

### The 38-track signature

| Grain | Finding |
|---|---|
| per customer | 58 of 59 bought **exactly 38 tracks** (one bought 36) |
| per month | 20 of 24 months sold **exactly 38 tracks** across exactly 7 orders |
| per country | `units = 38 × customers` — every country, no exceptions |
| spend | `38 × $0.99 = $37.62` — the exact spend of **30 of 59 customers** |

### The pricing identity

With only two price points, spend is fully determined by counts:

```
spend = 0.99 × (tracks bought) + 1.00 × (premium tracks bought)
```

The coefficient on premium tracks is **1.00**, not 1.99 — it is the
*difference* between the two prices. Price every track at $0.99, then add one
dollar for each that was actually $1.99.

We reconstructed every customer's spend from track counts alone and it matched
to the cent for all 59. This also explains why only 12 distinct spend values
exist, all exactly $1.00 apart.

### Consequence

Purchase volume is **fixed by construction**. The only variation anywhere in
the dataset is product mix — how many $1.99 video tracks happened to land in a
basket, a month, or a country.

This does not invalidate the pipeline. The star schema, DQ layer,
referential integrity checks and reconciliation all stand. It changes how we
*phrase* the business answers: we report "flat, and here is why" rather than
reading noise from a seven-order month as a trend.

<!-- ─────────── new %md cell ─────────── -->

## 7. Verification

Checks built into the pipeline, and what each proves.

| Check | Layer | Proves |
|---|---|---|
| Row counts per table | raw | every file landed |
| `rows = non_null_keys = distinct_keys` | raw | primary keys unique — replaces a dedup step |
| Completeness profile (every column) | raw / clean | which columns are populated, informs the DQ rules |
| Value distributions | raw | **caught the CSV parsing bug** |
| Referential integrity, 9 FKs | raw / clean | no orphan keys |
| `dq_status` breakdown | clean | how many rows are degraded and why |
| `SUM(line_amount)` vs `invoice_total` | clean | the fact reconciles to the invoice header |
| Clean revenue = mart revenue | mart | no rows lost to a join in the fact build |
| Fact FK orphans | mart | every fact row joins to all four dimensions |
| Dim `rows` vs `distinct keys` | mart | no dimension will fan out the fact |
| Revenue survives the full star join | mart | the star works end to end |

### Notes on two of them

**Orphans are not nulls.** A `track_id` of 9999 pointing at a track that does
not exist is populated, non-null, and still breaks the join. Null checks
cannot find it — only attempting the join can.

**Dimension key uniqueness is the one that would hurt most.** A duplicate key
in a dimension fans out the fact and multiplies revenue, silently. This is the
failure a dedup step would have prevented, so the assertion has to be read,
not just run.


<!-- ─────────── new %md cell ─────────── -->

## 8. Team process
- Conventions agreed before any SQL was written. Schema names (raw/clean/mart), table names, and column names were fixed up front so that work done in parallel would compose without rework. This is the most common failure mode in a shared notebook — two people naming the same concept differently — and agreeing it first cost minutes rather than a merge.
- Pre-cleaning analysis was split by table, two to three tables per member. Each member profiled their own tables: key uniqueness, completeness, value distributions, referential integrity.
- Whoever profiled a table also cleaned it. The person who found an issue proposed the cleaning step and the DQ flags for it, so context never had to be handed off. Someone who has looked at a column's actual distribution makes better decisions about it than someone reading a summary.
- Findings were consolidated into a shared document before cleaning was finalized. All issues found were shared during checkpoint meeting. 
- Dimensions were distributed only after the clean layer was done.


<!-- ─────────── new %md cell ─────────── -->

## 9. Business question answers

### Q1 — Top revenue by genre per country
 
**Rock is the top-revenue genre in 22 of 24 countries.**
 
| Genre ranked #1 | Countries |
|---|---|
| Rock | 22 |
| Alternative & Punk | 1 |
| TV Shows | 1 |
| Latin | 1 |
 
(25 rows across 24 countries — one country has a tie for first, so `RANK()`
correctly returns two rows for it.)
 
Leading markets, all on Rock:
 
| Country | Revenue | Units |
|---|---|---|
| USA | 155.43 | 157 |
| Canada | 105.93 | 107 |
| Brazil | 80.19 | 81 |
| France | 64.35 | 65 |
| Germany | 61.38 | 62 |
| United Kingdom | 36.63 | 37 |
 
Revenue equals exactly `$0.99 × units` in every row — no Rock track is
premium-priced.
 
#### The finding: purchases track the catalogue, not taste
 
Each genre's share of revenue closely matches its share of the catalogue:
 
| Genre | Tracks | % of catalogue | Revenue | % of revenue | Difference |
|---|---|---|---|---|---|
| Rock | 1,297 | 37.0 | 826.65 | 35.5 | −1.5 |
| Latin | 579 | 16.5 | 382.14 | 16.4 | −0.1 |
| Metal | 374 | 10.7 | 261.36 | 11.2 | +0.5 |
| Alternative & Punk | 332 | 9.5 | 241.56 | 10.4 | +0.9 |
| Jazz | 130 | 3.7 | 79.20 | 3.4 | −0.3 |
| TV Shows | 93 | 2.7 | 93.53 | 4.0 | +1.3 |
| Blues | 81 | 2.3 | 60.39 | 2.6 | +0.3 |
| Drama | 64 | 1.8 | 57.71 | 2.5 | +0.7 |
| Sci Fi & Fantasy | 26 | 0.7 | 39.80 | 1.7 | +1.0 |
 
Rock does not win because customers prefer it. It wins because it is 37% of
the catalogue. Every genre sells roughly in proportion to how many of its
tracks the store carries. The three non-Rock countries are single-customer
markets where one purchase decides the ranking.


### Q2 — Customer segmentation by spending tier

**Customers cannot be meaningfully segmented by spend.**

The brief's example thresholds (High > $50, Medium $20–50, Low < $20) place
all 59 customers in one tier — spend ranges only $36.64 to $49.62. We used
quartiles instead, which guarantee separation:

| Tier | Customers | Spend range | Revenue | % |
|---|---|---|---|---|
| Top 25% | 15 | 39.62 – 49.62 | 654.30 | 28.1% |
| Upper Mid | 15 | 37.62 – 39.62 | 584.30 | 25.1% |
| Lower Mid | 15 | 37.62 – 37.62 | 564.30 | 24.2% |
| Bottom 25% | 14 | 36.64 – 37.62 | 525.70 | 22.6% |

Four tiers, essentially equal revenue — against a typical retail pattern of
50–70% from the top quartile. **30 of 59 customers spent exactly $37.62** and
were split across three tiers by the bucketing, so the middle boundaries are
arbitrary.

We label the tiers "Top 25%" rather than "High" because `NTILE` measures
relative position, not an absolute threshold.

---

### Q3 — Monthly sales trend

**Flat, with no trend in either direction.**

Over the last two years (2024-01 to 2025-12): $928.11 revenue, 889 tracks,
163 orders — averaging $38.67 and 6.8 orders per month.

- 20 of 24 months: **exactly 38 tracks across exactly 7 orders**
- 16 of 24 months: revenue **exactly $37.62**
- 12 of 23 months: MoM growth **exactly 0.0%**

Only six months deviate, each for one of two reasons:

| Cause | Months |
|---|---|
| Premium content burst | 2024-07 (2 premium), 08 (10), 09 (18), 10 (5); 2025-11 (12), 12 (1) |
| Fewer tracks sold | 2024-09 (29), 2025-02 (28), 2025-04 (34) |

"Last 2 years" is anchored to the data's maximum date via a
`months_from_latest` column rather than a hardcoded date, so the view stays
correct if the data is extended.

---

### Q4 — Employee sales performance by quarter

**No sales employee outperforms another. The rankings measure customer
allocation.**
 
Across 20 quarters:
 
| Rep | Customers | Quarters won | Total revenue | Revenue / customer |
|---|---|---|---|---|
| Jane Peacock | 21 | 11 | 833.04 | 39.67 |
| Margaret Park | 20 | 8 | 775.40 | 38.77 |
| Steve Johnson | 18 | 4 | 720.16 | **40.01** |
 
(23 wins across 20 quarters — three quarters ended in ties, so `RANK()`
returns two rows for those.)
 
#### Two rankings, opposite orders
 
| By total revenue | By revenue per customer |
|---|---|
| 1. Jane Peacock | 1. **Steve Johnson** |
| 2. Margaret Park | 2. Jane Peacock |
| 3. Steve Johnson | 3. Margaret Park |
 
The rep who won the fewest quarters extracts the most from each account.
 
#### Share of revenue equals share of customers
 
| Rep | % of customers | % of revenue |
|---|---|---|
| Jane Peacock | 35.6 | 35.8 |
| Margaret Park | 33.9 | 33.3 |
| Steve Johnson | 30.5 | 30.9 |
 
Within 0.6 percentage points. Revenue is allocated, not earned. The books are
otherwise indistinguishable — every rep's customers bought **38 tracks across
7 orders** on average, the same signature seen at customer, month and country
level.
 
#### Why "quarters won" exaggerates the gap
 
Winning a quarter is a **threshold event** — first place or nothing. A rep
with a persistent small edge does not win slightly more often, they win
disproportionately more often, because the edge tips every close quarter their
way.
 
Jane holds five percentage points more customers than Steve and converts that
into 55% of wins against his 20%. Quarters won is in exact book-size order:
21 > 20 > 18 produces 11 > 8 > 4.

---

### Q5 — Popular tracks by quantity sold

**No track sold more than twice.**

| | Tracks | % of catalog |
|---|---|---|
| Sold 2 units | 256 | 7.3% |
| Sold 1 unit | 1,728 | 49.3% |
| **Never sold** | **1,519** | **43.4%** |
| Catalog total | 3,503 | |

A "top 20" is an arbitrary slice of the 256 tracks tied at the maximum. Our
first ranking surfaced eight TV episodes at the top purely because the
tiebreak sorted on revenue, favouring $1.99 video over $0.99 audio — changing
the tiebreak produces a different, equally valid list.

**The real finding at this grain: 43% of the catalog has never been
purchased.** That is our beyond-revenue theme — catalog coverage rather than
sales.

---

### Q6 — Regional pricing insights

**Unit prices do not differ by region.** One global price list, two price
points.

| Country | Customers | Units | Revenue | Premium units | Avg price |
|---|---|---|---|---|---|
| USA | 13 | 494 | 523.06 | 34 | 1.0588 |
| Germany | 4 | 152 | 156.48 | 6 | 1.0295 |
| France | 5 | 190 | 195.10 | 7 | 1.0268 |
| Brazil | 5 | 190 | 190.10 | 2 | 1.0005 |
| Canada | 8 | 304 | 303.96 | 3 | 0.9999 |
| UK | 3 | 114 | 112.86 | 0 | **0.9900** |

Average price is an **identity**, not a correlation:

```
avg_price = 0.99 + 0.01 × (% of units that were premium)
```

It reproduces every country's average to four decimals with no residual. The
UK proves it cleanest — zero video purchases, average exactly $0.99.

Underneath, `units = 38 × customers` in every country, so country revenue is a
direct function of headcount. The USA does not lead because Americans buy
more; it leads because 13 of 59 customers live there.

