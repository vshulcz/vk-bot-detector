# VK Bot Detection

Bot spotting toolkit for VK communities: a headless crawler, SQLite storage, feature engineering, and gradient boosted models that score every commenter.

## Highlights
- Full crawling pipeline (posts, comments, profiles) with adaptive rate limiting and session pooling.
- Deterministic feature builds that emit CSVs for network, profile, behavioral, and text signals.
- Notebook-first model training stack (LightGBM, XGBoost, CatBoost) with calibration, threshold tuning, and exportable predictions.
- Single SQLite file (`vk.sqlite`) acts as the contract between crawling and modeling stages.

## Repository layout

```
.
├── main.py                      # Entry point for quick crawls
├── crawler/                     # Scraper, session pool, pipeline orchestration
├── storage/                     # SQLAlchemy models and persistence helpers
└── detection/                   # Feature engineering + model training notebook
```

## Prerequisites
- Python 3.11+
- SQLite 3.40+
- Chromium (for Playwright based fallbacks)
- Optional: JupyterLab/VS Code for notebook execution

## Environment setup

```bash
git clone https://github.com/vshulcz/vk-bot-detector.git
cd vk-bot-detection
uv sync
uv playwright install chromium
```

## Crawling VK data
1. Edit `main.py` (or call `crawler.pipeline.run_pipeline_for_group`) and set:
	- `group_name`: list of VK short names to crawl.
	- `max_posts`, `max_comments_per_post`: per run budget.
	- `db_path`: target SQLite file (defaults to `vk.sqlite`).
2. Launch a crawl:

```bash
python main.py
```

Every run upserts data into `vk.sqlite` via SQLAlchemy models defined inside `storage/`.

### Fast tips
- `fast_mode=True` uses more aggressive timeouts and 12 workers (see `crawler/config.py`).
- Set `collect_from_db=True` when you only need to regenerate features from an existing SQLite dump.
- Logs land in `crawler.log` by default; raise `setup_logger(..., level=logging.DEBUG)` to debug throttling issues.

## Feature engineering pipeline
All feature logic lives in `detection/master_feature_engineering.py`. It connects to `vk.sqlite` and emits CSVs into `features_output/`:

| File | Description |
| --- | --- |
| `profile_features.csv` | Profile completeness, text richness, naming anomalies |
| `network_features.csv` | Interaction graph stats, reply depth, dialog diversity |
| `text_features.csv` | Comment level natural-language markers, spam scores |
| `complete_features.csv` | Joined table ready for modeling (166 columns) |

## Model training (notebook)
`detection/model_training.ipynb` documents the entire modeling workflow:
- Data audit.
- Train/validation split, cross-validation for LightGBM, XGBoost, CatBoost.
- Threshold tuning (best F1 at 0.674 → precision 0.863, recall 0.656).
- Calibration diagnostics (Brier score 0.0134) and hold-out metrics (LightGBM F1 0.758, ROC-AUC 0.960).
- Export of predictions for unlabeled accounts.

Recommended workflow:
1. Launch Jupyter: `jupyter lab detection/model_training.ipynb`.
2. Run cells sequentially (they re-load CSVs, so no cache artifacts).
3. Adjust hyperparameters or threshold grid as needed.
4. Save the trained booster or export probabilities (see the final notebook cell for file paths).

## Reproducing the full result
1. **Crawl** a representative VK community list into `vk.sqlite` (`python main.py`).
2. **Build features** (`python -m detection.master_feature_engineering`).
3. **Open the notebook** and execute all cells to train LightGBM/XGBoost/CatBoost.
4. **Save artifacts**: the notebook writes predictions and the tuned LightGBM model; copy them from `features_output/` or your chosen path.

## Troubleshooting
- VK rate limits: switch `fast_mode=False`, reduce worker count in `crawler/config.py`, and extend `REQUEST_SLEEP`.
- Playwright errors: ensure `playwright install chromium` was executed under the active virtual environment.
