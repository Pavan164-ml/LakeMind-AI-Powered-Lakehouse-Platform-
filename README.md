# LakeMind-AI-Powered-Lakehouse-Platform

## 1. Project Summary

Lake Mind is a fully local portfolio project that combines:

- Data engineering pipelines
- Medallion architecture
- Parquet data lake storage
- DuckDB analytics
- Airflow orchestration
- Streamlit query/chat UIs
- ChromaDB document retrieval
- Ollama-based local LLM answer generation
- A semantic layer for safer business Q&A

The project uses the Brazilian Olist e-commerce dataset and synthetic Olist-style markdown documents.

Important: the markdown policy/FAQ/SOP documents are fictional demo documents for portfolio use. They are not official Olist company policies.

## 2. High-Level Architecture

```text
Olist CSV Files
  -> PySpark Bronze ingestion
  -> data/bronze
  -> PySpark Silver transformations
  -> data/silver
  -> PySpark Gold KPI + star-schema transformations
  -> data/gold
  -> DuckDBQueryService
  -> FastAPI /query, /ask, /answer
  -> Streamlit Query UI and Chatbot UI

Synthetic Olist Markdown Docs
  -> document chunking
  -> sentence-transformer embeddings
  -> ChromaDB
  -> FastAPI /retrieve-docs, /ask, /answer
```

## 3. Core Design Principle

The LLM does not directly decide tables, joins, or metric formulas.

Instead:

```text
User question
  -> semantic router
  -> approved SQL from semantic catalog
  -> DuckDB result
  -> optional ChromaDB document retrieval
  -> Ollama summarizes approved context
```

This avoids common LLM mistakes:

- wrong table selection
- hallucinated columns
- incorrect joins
- inconsistent revenue or retention definitions
- unsupported metrics such as profit margin or marketing ROI

## 4. Tech Stack

- Python 3.11
- PySpark 3.5.1
- Parquet
- DuckDB
- ChromaDB
- sentence-transformers `all-MiniLM-L6-v2`
- FastAPI
- Streamlit
- Airflow in Docker
- Ollama with `phi3.5`
- Local Windows development environment

## 5. Data Sources

### Structured Data

Raw CSV source folder:

```text
Master/Olist Dataset
```

Core CSVs:

- customers
- geolocation
- orders
- order_items
- order_payments
- order_reviews
- products
- sellers
- category translation

### Document Data

Synthetic markdown documents:

```text
Master/Olist documents
```

Documents:

- `01_customer_service_policy.md`
- `02_seller_onboarding_sop.md`
- `03_shipping_logistics_policy.md`
- `04_data_governance_privacy_policy.md`
- `05_business_unit_overviews.md`
- `06_customer_faq.md`

## 6. Medallion Layers

### Bronze

Purpose:

- Preserve raw-ish source data as Parquet.
- Add technical metadata.
- Avoid business transformations.

Output:

```text
Master/project/data/bronze
```

Key script:

```text
src/ingestion/csv_ingestion.py
```

Airflow DAG:

```text
dags/bronze_ingestion_dag.py
```

### Silver

Purpose:

- Clean and type data.
- Standardize names/casing.
- Fix source typos.
- Join useful dimensions.
- Create item-level analytical table.

Output:

```text
Master/project/data/silver
```

Key script:

```text
src/transformations/silver_transformations.py
```

Important table:

```text
silver.order_details
```

Grain:

```text
one row per order item
```

Important warning:

`payment_value` is order-level and can repeat across item rows in `silver.order_details`. Do not sum it directly at item grain unless using allocation logic.

Airflow DAG:

```text
dags/silver_transformation_dag.py
```

### Gold

Purpose:

- Business-ready KPI tables.
- Star-schema-style facts and dimensions.
- Semantic-layer-ready tables for LLM and BI.

Output:

```text
Master/project/data/gold
```

Key script:

```text
src/transformations/gold_transformations.py
```

Airflow DAG:

```text
dags/gold_transformation_dag.py
```

## 7. Gold Tables

### KPI Tables

- `gold.executive_summary`
- `gold.sales_daily`
- `gold.category_performance`
- `gold.category_monthly_trends`
- `gold.payment_performance`
- `gold.customer_retention`
- `gold.geo_sales`
- `gold.seller_performance`
- `gold.review_performance`
- `gold.kpi_by_dimension`

### Star Schema Tables

- `gold.fact_order_item`
- `gold.dim_date`
- `gold.dim_customer`
- `gold.dim_product`
- `gold.dim_seller`
- `gold.dim_payment_type`
- `gold.dim_order_status`

### Metadata Table

- `gold.data_availability`

This table exposes:

- min order date
- max order date
- total order days
- total orders

## 8. Star Schema Purpose

The source ERD explains raw source relationships.

The Gold star schema helps with analytics.

```text
gold.fact_order_item
  -> gold.dim_date
  -> gold.dim_customer
  -> gold.dim_product
  -> gold.dim_seller
  -> gold.dim_payment_type
  -> gold.dim_order_status
```

Fact table:

- what happened
- order item sale
- revenue
- freight
- review score
- delivery delay

Dimension tables:

- how to slice/filter
- date
- product
- customer
- seller
- payment type
- order status

## 9. Key Metric Definitions

Total revenue:

```text
Sum of payment value at order level.
For product/category/seller item-level analysis, use allocated payment value to avoid double counting.
```

Average order value:

```text
total_revenue / total_orders
```

Cancellation rate:

```text
canceled_orders / total_orders
where order_status = 'canceled'
```

Repeat customer:

```text
customer_unique_id with more than one distinct order_id
```

Delivery delay:

```text
actual delivery date - estimated delivery date
negative value = delivered earlier than estimated
```

Unsupported metrics:

- true profit margin
- marketing ROI
- CAC
- LTV

Reason: the dataset does not include cost, marketing spend, or acquisition data.

## 10. Semantic Layer

The semantic layer is the governed business vocabulary for the chatbot and API.

Main config:

```text
config/semantic_catalog.yaml
```

Entity alias config:

```text
config/business_entities.yaml
```

Main docs:

```text
docs/semantic_catalog.md
docs/gold_kpi_tables.md
docs/hybrid_qa_design.md
```

Code:

```text
src/semantic/entity_resolver.py
src/semantic/dynamic_sql_builder.py
src/semantic/metric_catalog.py
src/semantic/sql_builder.py
src/qa/question_router.py
```

The semantic catalog defines:

- approved question patterns
- approved SQL
- recommended tables
- metric definitions
- document keywords
- fallback route

The entity resolver extracts:

- metrics such as `total_revenue`, `avg_order_value`, and `avg_delivery_delay_days`
- dimensions such as category, state, city, seller, payment type, and date grain
- locations such as São Paulo -> `SP`
- product category aliases such as electronics -> `computers_accessories`
- limits such as "top 10"

The dynamic SQL builder converts extracted entities into governed SQL using approved Gold tables and columns. For example:

```text
sales in sao paulo
  -> metric: total_revenue
  -> filter: customer_state = SP
  -> table: gold.geo_sales
```

```text
what are top products sold
  -> metric: total_items_sold
  -> dimension: product_label
  -> table: gold.fact_order_item joined to gold.dim_product
```

## 11. Query Layer

Main service:

```text
src/query/duckdb_service.py
```

Purpose:

- Discover Parquet tables under Bronze/Silver/Gold.
- Register DuckDB schemas:
  - `bronze`
  - `silver`
  - `gold`
- Run read-only SQL.
- Block unsafe SQL keywords.

Used by:

- Streamlit Query UI
- FastAPI `/query`
- FastAPI `/ask`
- FastAPI `/answer`

## 12. RAG Layer

Main file:

```text
src/ingestion/document_ingestion.py
```

Current responsibilities:

- Load markdown docs.
- Chunk policy/SOP docs by section.
- Chunk FAQ by Q&A pair.
- Chunk business unit overview by `---`.
- Embed chunks with sentence-transformers.
- Store chunks in ChromaDB.
- Retrieve relevant chunks.

ChromaDB storage:

```text
data/chromadb
```

Collection:

```text
olist_demo_documents
```

Evaluation scaffold:

```text
scripts/document_retrieval_eval.py
```

Future cleanup:

Split RAG code into:

- `src/rag/chunker.py`
- `src/rag/embedder.py`
- `src/rag/vector_store.py`
- `src/rag/retriever.py`

## 13. FastAPI Layer

Main file:

```text
src/api/main.py
```

Important endpoints:

- `GET /health`
- `POST /ingest`
- `POST /ingest/batch`
- `GET /tables`
- `POST /query`
- `GET /data-availability`
- `POST /retrieve-docs`
- `POST /ask`
- `POST /answer`

Endpoint purpose:

`/query`:

- Runs read-only SQL through DuckDB.

`/retrieve-docs`:

- Retrieves relevant document chunks from ChromaDB.

`/ask`:

- Routes the natural-language question.
- Returns structured result and/or retrieved docs.
- Does not call the LLM.

`/answer`:

- Calls router.
- Runs DuckDB and/or retrieval.
- Calls Ollama.
- Returns LLM answer plus structured result, SQL, sources, chart metadata, and metric definitions.

## 14. Streamlit UIs

Query editor:

```text
tools/query_ui.py
```

Purpose:

- Manually query Bronze/Silver/Gold.
- Browse tables.
- Run SQL.
- Validate outputs.

Chatbot UI:

```text
tools/chatbot_ui.py
```

Purpose:

- Chat-style UI over `/answer`.
- Shows LLM answer.
- Shows structured result.
- Shows SQL.
- Shows sources.
- Shows metric definitions.
- Shows chart recommendation and basic charts.
- Provides Ollama model dropdown from `/api/tags`.

## 15. Local LLM

Current model:

```text
phi3.5
```

Main service:

```text
src/llm/answer_service.py
```

Important rule:

The LLM is not the source of truth.

The source of truth is:

- structured DuckDB result
- retrieved document chunks
- semantic catalog definitions

The LLM only writes the final explanation.

Known issue:

Small local models may format or phrase numbers imperfectly. Keep structured rows visible in the UI.

## 16. Airflow

Airflow runs in Docker.

DAGs:

- `dags/bronze_ingestion_dag.py`
- `dags/silver_transformation_dag.py`
- `dags/gold_transformation_dag.py`

Purpose:

- Make ingestion and transformations repeatable.
- Validate row counts.
- Move from manual scripts to orchestrated pipelines.

## 17. How To Run

Start FastAPI:

```powershell
cd C:\Users\pavankumar65\Desktop\AI_Data_Platform\Master\project
$env:PYTHONPATH = (Get-Location).Path
uvicorn src.api.main:app --reload
```

Open Swagger:

```text
http://localhost:8000/docs
```

Run query UI:

```powershell
streamlit run C:\Users\pavankumar65\Desktop\AI_Data_Platform\Master\project\tools\query_ui.py
```

Run chatbot UI:

```powershell
streamlit run C:\Users\pavankumar65\Desktop\AI_Data_Platform\Master\project\tools\chatbot_ui.py
```

Run document ingestion:

```powershell
cd C:\Users\pavankumar65\Desktop\AI_Data_Platform\Master\project
$env:PYTHONPATH = (Get-Location).Path
python -m src.ingestion.document_ingestion --query "What is the return window for electronics?" --top-k 3
```

## 18. Current Validation Status

Completed:

- Bronze ingestion for all 9 CSVs.
- Bronze Airflow DAG.
- Silver transformations.
- Silver Airflow DAG.
- Gold KPI transformations.
- Enhanced Gold star schema tables.
- Gold Airflow DAG.
- DuckDB query service.
- Streamlit query UI.
- Document ingestion and retrieval.
- FastAPI `/query`, `/retrieve-docs`, `/ask`, `/answer`.
- Ollama `phi3.5` answer generation.
- Streamlit chatbot UI.

Needs continued testing:

- Retrieval evaluation expected sources.
- Answer quality across many question types.
- UI chart behavior for all response types.
- Model speed and quality.
- FastAPI automated tests.

## 19. Known Limitations

- Product names are not available in the raw Olist dataset. The project uses product category plus short product ID labels.
- Synthetic Olist documents are fictional and only for portfolio/demo RAG.
- True profit margin and marketing ROI are unsupported due to missing cost/marketing data.
- Local LLM may misformat numbers in prose; structured result remains authoritative.
- The RAG code currently lives mostly in `src/ingestion/document_ingestion.py`; it should later be split into dedicated `src/rag` modules.
- `fastapi.testclient.TestClient` has a local dependency mismatch with Starlette/httpx; endpoint functions were validated directly.

## 20. Best Next Steps

1. Fill expected sources in `scripts/document_retrieval_eval.py`.
2. Run retrieval evaluation and tune chunking if needed.
3. Test chatbot UI with structured, document, and hybrid questions.
4. Improve answer prompts and chart rendering.
5. Split RAG implementation into dedicated `src/rag` modules.
6. Add automated tests for router, SQL builder, query service, and API endpoints.
7. Add conversation memory only after single-turn answers are stable.

## 21. Deeper Reference Docs

Use this guide first. Then use these for detail:

- `docs/gold_kpi_tables.md`
- `docs/semantic_catalog.md`
- `docs/hybrid_qa_design.md`
- `docs/project_progress.md`
- `docs/project_learning.md`
- `Master/plans/olist_data_model_design.md`
- `Master/plans/LAKE_MIND_MASTER_PLAN.md`
