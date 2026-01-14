# Inquiry data — onboarding notebooks 

**Short summary**  
I explored the Equifax *inquiry* data and implemented two notebook variants that load, clean, deduplicate and turn inquiry rows into model-ready features. The work follows the guidance in the `inquiry.json` asset (used as inspiration). :contentReference[oaicite:0]{index=0}

---

## Files in this folder

- `functions_for_onboarding.py`  
  Reusable helper functions used by both notebooks (S3 helpers, date cleaning, feature-builder). The main feature-builder in the file is `build_final_inquiry_features(...)` which computes counts, gap statistics, percentiles, windowed counts (1/3/6/9/12 months), and decay-weighted counts (30d/90d half-life).

- `Inquiry_Table_Ind_Code_Dedup_Method.ipynb`  
  **Approach (A — grouped dedupe)**  
  - Treats _auto_ & _mortgage_ as a single `auto_mortgage` group for the credit-shopping deduplication step (this mirrors the conservative asset grouping).  
  - Uses `indCode` = `auto_mortgage` / `other` for feature generation.  
  - Produces `all_` features and `auto_mortgage` vs `other` features: counts, mean gap, gap percentiles (p10/p25/p50/p75/p90), gap CV, days-since-last, window counts, and decay-weighted counts.  


- `Inquiry_Table_New_Dedup_Method.ipynb`  
  **Approach (B — split dedupe)**  
  - Splits `auto` and `mortgage` into separate product buckets and deduplicates shopping clusters **separately** for `auto` and `mortgage` (45-day rule within each product).  
  - Generates features for `auto`, `mortgage`, `other`, and `all` (same metrics as above) so you get more granular product-specific signals.  
---

## What the notebooks do (common steps)

1. **Load data** — read parquet from S3 (helper `list_s3_files()` + `load_df_from_list()`), take a single quarter for inspection.  
2. **Clean dates** — normalize and parse noisy date fields (`clean_up_date_column()`), produce `DATE_OF_INQUIRY_CLEANED` etc.  
3. **Map product codes** — map `CUSTOMER_NUMBER` → `inquiry_product_type` (`auto`, `mortgage`, `other` or fallback `auto_mortgage`) using the categorical mapping.  
4. **Dedupe shopping events** — identify clusters of inquiries (45-day window), optionally grouped by `auto`/`mortgage` vs combined `auto_mortgage`. Keep first inquiry per cluster (the notebooks differ here).  
5. **Feature engineering** — run `build_final_inquiry_features(...)` which:
   - computes `*_num_inq`, `*_mean_gap_days`, `*_std_gap_days`, `*_gap_cv`, `*_gap_p10/p25/p50/p75/p90`, `*_days_since_last_inquiry`, `*_months_since_last_inquiry`,
   - computes window counts `*_inq_1m` / `*_inq_3m` / `*_inq_6m` / `*_inq_9m` / `*_inq_12m`,
   - computes decay-weighted counts (`*_decay_30d`, `*_decay_90d`) where each inquiry is weighted by `2^(−age_days / H)`,
   - and returns a single wide table keyed by `ZEST_KEY`, containing `all_` block plus product-prefixed blocks when requested.

---

## Quick usage / run (example) (WORK IN PROGRESS)

```python
# in a notebook cell or script
from functions_for_onboarding import list_s3_files, load_df_from_list, clean_up_date_column, build_final_inquiry_features

# 1) Load a partition (example)
bucket = "power-client-data-staging"
prefix = "CLIENT/PARSED/DATA/BUREAU=equifax/FORMAT=cms_6/TABLE=inquiry/PULL_NAME=20250201_oefcu_orangecounty_orlando_trustone_vantagewest/ARCHIVE_DATE=2025-01-31/"
files = list_s3_files(bucket, prefix)
df = load_df_from_list(files, 0)   # load first parquet file

# 2) Clean inquiry date
df = clean_up_date_column(df, 'DATE_OF_INQUIRY')
#
## From here, choose how you want to break up CUSTOMER_NUMBER for de-duplicating, 

