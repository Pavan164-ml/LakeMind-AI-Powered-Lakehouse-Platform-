# LakeMind — GenAI + Data Engineering Interview Preparation

> **Generated from actual repository inspection** (`Master/project/`).  
> Architecture reference doc: `docs/lake_mind_architecture_and_interview_guide.md` (not `project_architecture.md` — that file does not exist).

---

## 1. Repository Understanding

**LakeMind** (code name: "Lake Mind") is a **local, portfolio-grade AI data platform** over the Olist Brazilian e-commerce dataset. It combines:

- **Medallion data engineering** (Bronze → Silver → Gold Parquet via PySpark)
- **DuckDB** for governed analytical SQL over Parquet
- **ChromaDB + sentence-transformers** for dense retrieval over synthetic markdown docs
- **Ollama (phi3.5)** for question routing and grounded answer composition
- **FastAPI** API + **Streamlit** UIs

**What it is NOT:** a production multi-tenant SaaS, an agentic framework, or free-form text-to-SQL. The LLM **never writes SQL** — it classifies intent and summarizes pre-fetched context.

**Primary codebase:** `Master/project/`

**Stale documentation to watch:** README roadmap (Phase 2b marked "coming soon" but implemented), references to `src/llm/llm_service.py` and `src/rag/` modules that don't exist, config values for HuggingFace phi-3-mini that aren't used at runtime.

---

## 2. Actual Architecture

```text
User (Streamlit chatbot OR HTTP client)
 ↓
FastAPI (src/api/main.py) — POST /answer | /ask | /query | /retrieve-docs
 ↓
question_router.route_question() (src/qa/question_router.py)
 ↓
 ┌─────────────────────────────────────────────────────────────┐
 │ classify_question() — Ollama JSON routing                   │
 │   OR keyword/entity fallback if Ollama fails                │
 └─────────────────────────────────────────────────────────────┘
 ↓
 ┌──────────────────────┬──────────────────────────────────────┐
 │ Structured path      │ Document path                        │
 │                      │                                      │
 │ extract_entities()   │ has_document_signal() / LLM flag     │
 │ build_dynamic_sql()  │ retrieve_relevant_docs()             │
 │   OR catalog route   │   → SentenceTransformer encode       │
 │   from semantic_     │   → ChromaDB collection.query        │
 │   catalog.yaml       │                                      │
 │                      │                                      │
 │ DuckDBQueryService   │                                      │
 │   read_parquet views │                                      │
 └──────────┬───────────┴──────────────┬───────────────────────┘
            ↓                          ↓
     structured_rows            retrieved_documents
            └──────────┬───────────────┘
                       ↓
              compose_answer() — build_prompt() → call_ollama()
                       ↓
              Answer + chart metadata + sources + metric definitions
```

**Offline data pipeline (separate from API request path):**

```text
Olist CSVs → csv_ingestion.py (Spark) → data/bronze/*.parquet
         → silver_transformations.py → data/silver/*.parquet
         → gold_transformations.py → data/gold/*.parquet (18 tables)

Synthetic .md docs → document_ingestion.py → ChromaDB (data/chromadb)
```

**Optional orchestration:** Airflow DAGs in `dags/` via `docker/airflow/docker-compose.yaml`.

---

## 3. Technology Inventory

| Component | Status | File(s) | Technology | Purpose | Input | Output | Key Config |
|-----------|--------|---------|------------|---------|-------|--------|------------|
| CSV ingestion | **IMPLEMENTED** | `src/ingestion/csv_ingestion.py` | PySpark 3.5.1 | Bronze layer | Olist CSVs | Parquet + metadata cols | `config.yaml` spark.* |
| Silver transform | **IMPLEMENTED** | `src/transformations/silver_transformations.py` | PySpark | Clean, type, join | Bronze Parquet | Silver Parquet | overwrite mode |
| Gold transform | **IMPLEMENTED** | `src/transformations/gold_transformations.py` | PySpark | Star schema, KPIs | Silver Parquet | 18 Gold tables | `docs/gold_kpi_tables.md` |
| Parquet storage | **IMPLEMENTED** | `data/bronze|silver|gold/` | Parquet | Columnar lake | Spark writes | DuckDB reads | folder-per-table |
| DuckDB analytics | **IMPLEMENTED** | `src/query/duckdb_service.py` | DuckDB 1.1.3 | SQL over Parquet | Parquet globs | QueryResult | in-memory `:memory:` at API |
| Document parser | **IMPLEMENTED** | `src/ingestion/document_ingestion.py` | regex/markdown | Section-aware chunking | 6 `.md` files | DocumentChunk list | `Master/Olist documents/` |
| Chunker | **IMPLEMENTED** | same | Section-based | NOT fixed token size | markdown text | chunks w/ metadata | ignores config chunk_size |
| Embeddings | **IMPLEMENTED** | `document_ingestion.py` | all-MiniLM-L6-v2 | 384-dim vectors | chunk text | float[] | `config.yaml` embedding.* |
| Vector DB | **IMPLEMENTED** | `document_ingestion.py` | ChromaDB 0.5.23 | Similarity search | embeddings | top-k chunks | `olist_demo_documents` hardcoded |
| Retrieval | **IMPLEMENTED** | `retrieve_relevant_docs()` | Dense only | Query → embed → query | user question | docs + distance | top_k default 3-5 |
| Query router | **IMPLEMENTED** | `src/qa/question_router.py` | Ollama + YAML rules | Route structured/doc/hybrid/chat | NL question | RoutedQuestion | `semantic_catalog.yaml` |
| Semantic layer | **IMPLEMENTED** | `src/semantic/*`, YAML configs | Governed SQL | Pre-approved routes + dynamic filters | entities/keywords | SQL string | 11 catalog routes |
| LLM | **IMPLEMENTED** | `src/llm/answer_service.py` | Ollama phi3.5 | Summarize context | prompt | prose answer | temp=0.1, num_predict=512 |
| Routing LLM | **IMPLEMENTED** | `src/llm/routing_service.py` | Ollama (same/different model) | JSON classification | question + routes | RoutingDecision | `OLLAMA_ROUTER_MODEL` |
| FastAPI | **IMPLEMENTED** | `src/api/main.py` v0.3.0 | FastAPI 0.115.6 | HTTP API | JSON requests | JSON responses | port 8000 |
| Streamlit SQL UI | **IMPLEMENTED** | `tools/query_ui.py` | Streamlit | Direct DuckDB | user SQL | table/chart | bypasses router |
| Streamlit chat | **IMPLEMENTED** | `tools/chatbot_ui.py` | Streamlit | Calls `/answer` | chat message | rendered answer | API URL config |
| Airflow | **IMPLEMENTED** (optional) | `dags/*.py`, `docker/airflow/` | Airflow 2.9.3 | Batch orchestration | scheduled | pipeline runs | Docker compose |
| Docker (full stack) | **PARTIAL** | root `docker-compose.yml` | — | Empty stub | — | — | comments only |
| Tests | **PARTIAL** | `scripts/test_*.py` | pytest-style scripts | Manual validation | — | pass/fail | no CI |
| RAG evaluation | **PARTIAL** | `scripts/document_retrieval_eval.py` | Manual checklist | Retrieval smoke test | query list | printed matches | no MRR/NDCG |
| LangChain | **DOCUMENTED ONLY** | `requirements.txt` | — | Listed dependency | — | — | zero imports in src |
| src/rag/ package | **DOCUMENTED ONLY** | `src/rag/__init__.py` | — | Empty placeholder | — | — | logic in document_ingestion |
| HuggingFace local LLM | **DOCUMENTED ONLY** | `config.yaml` llm.* | — | Legacy config | — | — | runtime uses Ollama |
| BM25 / hybrid retrieval | **PRODUCTION TARGET** | — | — | Not implemented | — | — | dense only |
| Reranking | **PRODUCTION TARGET** | — | — | Not implemented | — | — | — |
| Auth / rate limiting | **PRODUCTION TARGET** | — | — | Not implemented | — | — | — |
| NL→SQL (LLM writes SQL) | **PRODUCTION TARGET** | — | — | Explicitly avoided | — | — | governed SQL only |
| Incremental ingestion | **PRODUCTION TARGET** | — | — | overwrite mode only | — | — | roadmap |
| Embedding model cache | **PRODUCTION TARGET** | — | — | Reloads model each retrieval | — | — | perf gap |

---

## 4. End-to-End Execution Flow

### Structured question: "Which product category generated the highest revenue?"

| Step | What happens | Code location |
|------|----------------|---------------|
| 1. Entry | `POST /answer` with `{"question": "..."}` | `main.py` → `answer_question()` |
| 2. Routing | `route_question()` calls `classify_question()` → Ollama returns JSON with `question_type: structured`, `route_name: category_performance` | `question_router.py`, `routing_service.py` |
| 3. Structured signal | No document keywords; entities may match "category" + "revenue" | `entity_resolver.py`, `semantic_catalog.yaml` |
| 4. SQL selection | LLM route validated → `route.sql` from catalog (ORDER BY total_revenue DESC LIMIT 10) OR `build_dynamic_sql()` if entities present | `sql_builder.py`, `dynamic_sql_builder.py` |
| 5. Execution | New `DuckDBQueryService`, auto-registers `gold.category_performance` view from Parquet | `duckdb_service.py` |
| 6. Guard | Read-only check blocks INSERT/UPDATE/DROP | `execute_query()` validation |
| 7. Result | Rows returned as JSON (top category first) | `QueryResponse` |
| 8. Retrieval | Skipped unless `use_documents=true` | — |
| 9. LLM | `build_prompt()` injects authoritative facts + structured rows; `call_ollama()` summarizes | `answer_service.py` |
| 10. Response | Answer text + chart `{type: bar}` + sources `{tables: [gold.category_performance]}` | `AnswerResponse` |

**Failure modes:**
- Ollama down → routing falls back to keyword/entity logic (`question_router.py` except block)
- No matching route → `fallback_semantic_route()` → `gold.executive_summary`
- SQL error → HTTP 500 from `/ask`
- Ollama unreachable on `/answer` → HTTP 503 with pull-model message

### Document question: "What is the return policy for electronics?"

| Step | What happens |
|------|----------------|
| 1-2 | Router classifies as `document`; `use_documents=true` |
| 3 | `needs_document_retrieval=True`; structured SQL may be skipped |
| 4 | `retrieve_relevant_docs(question, k=top_k)` |
| 5 | `get_embedding_model()` loads SentenceTransformer (no singleton) |
| 6 | Query embedded with `normalize_embeddings=True` |
| 7 | Chroma `collection.query(n_results=k)` — cosine distance on normalized vectors |
| 8 | Top chunks passed to LLM prompt (max 5 in prompt) |
| 9 | LLM instructed to cite synthetic demo docs, not invent policy |

### Hybrid: "Compare SP delivery delays with the shipping SLA in our policy docs"

| Step | What happens |
|------|----------------|
| 1 | `question_type=hybrid` — both structured SQL and retrieval |
| 2 | Dynamic SQL if entities include `SP` state + delivery metric |
| 3 | Document retrieval for SLA/policy chunks |
| 4 | LLM merges both contexts under strict grounding rules |

---

## 5. High-Risk Interview Areas

### 5.1 "The LLM generates SQL" (WRONG — high bluff risk)

- **Why risky:** README/architecture diagrams imply "natural language querying"; interviewers will probe text-to-SQL.
- **What to understand:** SQL comes from **YAML catalog routes** or **`build_dynamic_sql()`** from extracted entities. LLM only picks route name in JSON.
- **Likely questions:** "Show me the prompt that generates SQL." / "What if the LLM hallucinates a JOIN?"
- **Expected depth:** Quote `routing_service.py` docstring and `answer_service.py` lines 3-4.
- **Study:** `semantic_catalog.yaml`, `dynamic_sql_builder.py`, `routing_service.py`

### 5.2 Config vs runtime mismatch

- **Why risky:** `config.yaml` says phi-3-mini, chunk_size 500, collection `rag_documents` — none used at runtime.
- **Study:** `config.yaml`, `document_ingestion.py`, `answer_service.py` (DEFAULT_MODEL = phi3.5)

### 5.3 Empty `src/rag/` package

- **Why risky:** README lists `src/rag/` as RAG module; it's empty.
- **Say:** "RAG lives in `document_ingestion.py` — ingestion and retrieval colocated for the demo."

### 5.4 Embedding model reload on every query

- **Why risky:** Obvious performance flaw; senior engineers will ask about latency.
- **Study:** `retrieve_relevant_docs()` calls `get_embedding_model()` each time.

### 5.5 DuckDB in-memory vs persistent

- **Why risky:** Config has `database_path: data/querylake.duckdb` but API uses `:memory:`.
- **Study:** `duckdb_service.py` line 56, `get_query_service()` in `main.py`

### 5.6 Gold layer metric definitions (double-counting)

- **Why risky:** `allocated_payment_value` vs raw payment — revenue questions need precise definition.
- **Study:** `gold_transformations.py`, `METRIC_DEFINITIONS` in `answer_service.py`

### 5.7 Synthetic documents disclaimer

- **Why risky:** RAG corpus is 6 demo markdown files, not real Olist policies.
- **Study:** `document_ingestion.py` module docstring, prompt rule "say they are demo documents"

### 5.8 No reranking, no hybrid retrieval, no metadata filters at query time

- **Why risky:** Standard RAG interview drill-down.
- **Be honest:** Dense retrieval only; metadata extracted at ingest but not used in `collection.query(where=...)`.

---

## 6. 30-Second Project Explanation

> **LakeMind** is a local AI data platform I built over the Olist e-commerce dataset. CSVs flow through a Spark medallion pipeline into Parquet, queried by DuckDB with a YAML-governed semantic layer — not free-form text-to-SQL. Synthetic markdown docs are chunked, embedded with MiniLM, and stored in ChromaDB. A FastAPI backend routes questions via Ollama into structured SQL, document retrieval, or both, then grounds the LLM answer on actual query results and retrieved chunks.

---

## 7. 2-Minute Project Explanation

LakeMind solves the problem of **asking business questions across structured analytics and unstructured policy/FAQ documents** in one interface, without standing up a cloud data warehouse.

**Data engineering side:** Nine Olist CSVs ingest into Bronze Parquet preserving raw strings, Silver cleans and types data (including tricky Brazilian timestamps), Gold builds 18 analytical tables — executive summary, category performance, geo sales, seller performance, etc. Airflow DAGs can orchestrate this, but Spark also runs locally on Windows for development.

**Analytics side:** DuckDB registers each Parquet folder as a schema-qualified view (`gold.executive_summary`). The API enforces read-only SQL. Analysts can also use a Streamlit SQL editor that bypasses the router entirely.

**GenAI side:** Six synthetic markdown documents (policies, FAQ, SOPs) are split by markdown sections and Q&A pairs, embedded with `all-MiniLM-L6-v2`, and stored in ChromaDB. When a user asks a question, Ollama first classifies it as chat, structured, document, or hybrid. Structured questions map to pre-approved SQL routes or dynamically filtered SQL from entity extraction (year, state, category aliases from YAML). Document questions trigger dense vector retrieval. The answer LLM never chooses tables or invents metrics — it summarizes the rows and chunks already fetched, with temperature 0.1 and explicit grounding rules.

**Key trade-off:** I optimized for **governed correctness and local demoability** over scale and retrieval sophistication. No BM25, no reranker, no auth — those are explicit production next steps.

---

## 8. Master Interview Question Bank

### 8.1 Basic (30)

1. What is LakeMind?
2. What dataset does it use?
3. What is the medallion architecture in your project?
4. What are the three Parquet layers called?
5. What does the Bronze layer store?
6. What happens in the Silver layer?
7. What Gold tables exist?
8. What is DuckDB used for?
9. What is ChromaDB used for?
10. What embedding model do you use?
11. What LLM do you use at runtime?
12. What framework is the API built with?
13. What UI tools exist?
14. How many synthetic documents are in the RAG corpus?
15. What is the `/health` endpoint?
16. What is the difference between `/ask` and `/answer`?
17. What question types can the router return?
18. What is a semantic catalog in your project?
19. Where is Parquet stored?
20. Does the LLM write SQL in your system?
21. What is RAG in one sentence?
22. What port does the API run on?
23. What is the Olist dataset about?
24. What does `POST /query` do?
25. What does `POST /retrieve-docs` do?
26. What is the purpose of `GET /tables`?
27. What is `GET /data-availability` for?
28. Is Spark running in Docker?
29. What Python version targets the project?
30. What makes this different from ChatGPT with a CSV upload?

### 8.2 Intermediate (40)

31. Walk through `POST /answer` step by step.
32. How does `DuckDBQueryService` discover tables?
33. What read-only guards exist on SQL?
34. How are CSV column names normalized at ingest?
35. Why preserve timestamps as VARCHAR in Bronze?
36. How does `parse_markdown_sections()` chunk documents?
37. How are FAQ documents chunked differently?
38. How are chunk IDs generated and why SHA-256?
39. What metadata is attached to each chunk?
40. How does `normalize_embeddings=True` affect similarity?
41. What distance metric does Chroma return?
42. How does keyword fallback routing work?
43. What happens when Ollama is down during routing?
44. What entities can `extract_entities()` detect?
45. When does `build_dynamic_sql()` run vs catalog routes?
46. What is `should_use_dynamic_sql()` logic for hybrid?
47. How does `select_semantic_route()` match keywords?
48. What is the fallback route when nothing matches?
49. How are chart types recommended?
50. What metric definitions are injected in responses?
51. How does the chat prompt differ from analytics prompt?
52. What is in `business_entities.yaml`?
53. How many routes are in `semantic_catalog.yaml`?
54. What document keywords trigger retrieval?
55. How do you prevent SQL injection?
56. How do you prevent write operations via `/query`?
57. What is `allocated_payment_value` and why?
58. How does batch ingestion work?
59. What Airflow DAGs exist?
60. How would you re-ingest documents to Chroma?
61. What happens on API restart — is Chroma data persisted?
62. What happens on API restart — are DuckDB views recreated?
63. How does Streamlit chatbot call the backend?
64. Why separate routing and answer LLM calls?
65. What JSON does the router LLM return?
66. How is malformed router JSON handled?
67. What rows are passed to the LLM (limits)?
68. What is `json_safe_rows()` for?
69. Why is LangChain in requirements but unused?
70. What is the collection name in Chroma vs config?

### 8.3 Advanced (50)

71. Explain the full hybrid routing decision tree in `route_question()`.
72. How would wrong LLM route_name be corrected?
73. Design metadata-filtered retrieval for category-specific policies.
74. Why cosine similarity with normalized embeddings?
75. What breaks if you switch embedding models without re-indexing?
76. Compare your semantic layer to LookML/dbt metrics layer.
77. How does `dynamic_sql_builder` construct WHERE clauses?
78. What SQL validation would you add for production?
79. How would you implement query result caching?
80. How would you cache the embedding model singleton?
81. Trace memory usage for a single `/answer` request.
82. Why in-memory DuckDB vs file-backed?
83. How does Spark session config differ on Windows?
84. Explain Gold star schema design choices.
85. How is double-counting prevented in revenue metrics?
86. What is idempotency of Bronze ingest (overwrite mode)?
87. How would incremental Bronze ingest work?
88. Schema evolution if a new CSV column appears?
89. Small files problem in your Parquet layout?
90. Partition strategy — do you partition? Should you?
91. How would you add Delta Lake instead of raw Parquet?
92. Evaluate retrieval with Hit@3 on your eval script.
93. How to detect router misclassification in logs?
94. Prompt injection via synthetic policy documents?
95. How to prevent LLM from ignoring grounding rules?
96. Temperature 0.1 — when would you increase it?
97. num_predict 512 — what gets truncated?
98. Separate OLLAMA_ROUTER_MODEL — why?
99. How would you version `semantic_catalog.yaml`?
100. A/B test a new chunking strategy?
101. Compare section chunking vs 500-token sliding window.
102. What recall issues arise from section-only chunking?
103. How would you add cross-encoder reranking?
104. Implement BM25 + dense hybrid — where in code?
105. How does Chroma HNSW index work (conceptually)?
106. top_k=3 vs top_k=10 — trade-offs in your corpus?
107. Empty retrieval — what does LLM do today?
108. Conflicting chunks in context — current behavior?
109. How to add citation spans to answers?
110. Multi-turn conversation — supported? How not?
111. Async FastAPI workers for Ollama calls?
112. Connection pooling for DuckDB?
113. Horizontal scaling blockers?
114. Replace Ollama with OpenAI — what changes?
115. Cost model for 100x query volume?
116. Observability: what would you log per request?
117. Trace ID across router → SQL → retrieval → LLM?
118. Dead letter queue for failed ingestions?
119. Data quality checks between Silver and Gold?
120. Great Expectations / Soda — fit?

### 8.4 Architecture / System Design (30)

121. Draw LakeMind architecture from memory.
122. Boundaries between DE and GenAI components?
123. Stateful vs stateless components?
124. Single points of failure?
125. Bottlenecks at 10 concurrent users?
126. Redesign for 10,000 concurrent users.
127. Event-driven vs request-driven — your choice?
128. Where would a message queue fit?
129. CQRS pattern — applicable?
130. Microservices split — would you?
131. API gateway layer needed?
132. Separate retrieval service?
133. Feature store — relevant?
134. Data catalog integration (Amundsen/DataHub)?
135. Lineage from CSV → Gold → answer?
136. Multi-tenant document isolation design?
137. Blue/green deploy for semantic catalog changes?
138. Disaster recovery for Chroma + Parquet?
139. CI/CD pipeline design for this repo?
140. IaC for Airflow + API + Ollama?
141. Kubernetes vs single VM for demo?
142. Edge deployment (offline analytics)?
143. Federated query across lakes?
144. Real-time vs batch — current and future?
145. Lambda architecture fit?
146. Data mesh — overkill?
147. Replace Spark with Polars/DuckDB ingest?
148. Unified query engine (Trino) instead of DuckDB?
149. Lakehouse format (Iceberg) migration plan?
150. Zero-trust security model sketch?

### 8.5 RAG-Specific (25)

151. Why RAG vs fine-tuning for Olist policies?
152. What exactly is retrieved — chunks or docs?
153. How is context ordered in the prompt?
154. Max context chunks to LLM?
155. Dense-only limitations in your demo?
156. When would hybrid search help Olist FAQ?
157. Reranker placement in pipeline?
158. Chunk overlap — why you don't have it?
159. Parent-document retrieval pattern?
160. Hypothetical document embeddings (HyDE)?
161. Query expansion strategies?
162. MMR for diversity in top-k?
163. Faithfulness vs relevance metrics?
164. RAGAS framework applicability?
165. Golden dataset design for 6 docs?
166. Regression test when corpus doubles?
167. Embedding normalization — mathematical effect?
168. Negative retrieval examples?
169. Source attribution quality today?
170. Handle "I don't know" when no chunks match?
171. Adversarial corpus injection test?
172. Multilingual docs — MiniLM limitations?
173. Table-aware chunking for markdown tables?
174. Version document chunks on edit?
175. Embedding batch size for 1M chunks?

### 8.6 LLM (20)

176. Why Ollama locally?
177. Why phi3.5 specifically?
178. Context window constraints with your prompt size?
179. Two LLM calls per `/answer` — latency impact?
180. JSON routing reliability of small models?
181. Structured output vs tool calling?
182. Guardrails beyond prompt instructions?
183. NeMo / Guardrails / LlamaGuard fit?
184. Model fallback hierarchy?
185. Quantization — config says true, Ollama handles?
186. GPU vs CPU inference path?
187. Streaming responses — not implemented, how?
188. Token usage estimation?
189. System vs user prompt — you only use single prompt?
190. Few-shot examples in routing prompt?
191. Compare phi3.5 vs GPT-4o for routing accuracy?
192. Local model eval benchmark approach?
193. Fine-tune router as classifier?
194. Distillation from larger model?
195. LLM observability (LangSmith etc.)?

### 8.7 Agentic AI (20)

196. Is LakeMind an agent system?
197. Why no tool-calling loop?
198. ReAct pattern — would it help?
199. LangGraph orchestration fit?
200. Multi-step SQL agent risks?
201. Self-correction on SQL error?
202. Planner + executor split?
203. Human-in-the-loop for SQL approval?
204. Agent memory for chat sessions?
205. MCP servers for DuckDB/Chroma tools?
206. When agents beat your router+catalog?
207. Autonomous ingest agent — use case?
208. Evaluation of agent trajectories?
209. Cost of 5-step agent vs 2-call pipeline?
210. Safety: agent with write access — never?
211. Compare to Microsoft Semantic Kernel?
212. Compare to LlamaIndex agents?
213. CrewAI for DE + analyst roles?
214. Agent bluff detection in interviews?
215. Your design philosophy: workflow vs agent?

### 8.8 Data Engineering (20)

216. PySpark local[*] limitations?
217. shuffle_partitions=4 — when to tune?
218. PERMISSIVE CSV mode — risks?
219. Metadata columns added at Bronze?
220. Silver typing strategy for dates?
221. Join grain in order_details?
222. Gold fact vs dimension tables list?
223. Denormalization choices in Gold?
224. Data skew in Olist orders?
225. Orchestration dependency graph?
226. Backfill strategy for one table?
227. Data retention policy?
228. PII in Olist — handling?
229. LGPD mentioned in docs — implementation?
230. winutils/Hadoop on Windows — why?
231. Checkpoint dir purpose?
232. Parquet compression codec used?
233. Column pruning benefit with DuckDB?
234. Predicate pushdown in your stack?
235. Compare medallion to data vault here?

### 8.9 Production / Scalability (20)

236. First thing that breaks at 1M documents?
237. Chroma at 100M vectors?
238. Ollama single-thread bottleneck?
239. FastAPI sync endpoints — problem?
240. Embedding batch pipeline for bulk ingest?
241. Separate read replicas for analytics?
242. CDN for static Parquet — nonsense or not?
243. Auto-scaling API pods?
244. Model serving with vLLM/TGI?
245. Warm vs cold start for MiniLM?
246. SLI/SLO for `/answer` p95 latency?
247. Circuit breaker for Ollama?
248. Graceful degradation without LLM?
249. Canary deploy new semantic routes?
250. Feature flags for hybrid retrieval?
251. Load test tool choice?
252. Capacity planning worksheet?
253. Multi-region disaster recovery?
254. Cost of S3 + Athena vs current?
255. When to move off local Spark?

### 8.10 Security / Reliability / Cost (20)

256. Authn/z for `/query` exposure?
257. Prompt injection via user question?
258. SQL injection despite read-only?
259. Data exfiltration via LLM context?
260. Tenant isolation for Chroma collections?
261. Secrets management for Ollama URL?
262. API rate limiting design?
263. Audit log for executed SQL?
264. PII redaction in logs?
265. OWASP LLM Top 10 relevance?
266. Malicious markdown in doc corpus?
267. Supply chain: sentence-transformers model?
268. Dependency pinning strategy?
269. Cost of cloud embeddings at scale?
270. Cost of GPT-4 vs local phi3.5?
271. Storage cost Parquet vs warehouse?
272. Idle cost of Airflow stack?
273. ROI of caching frequent questions?
274. Budget alert design?
275. Threat model one-pager?

### 8.11 Adversarial (20)

276. "You just wrapped ChatGPT around CSVs." Response?
277. "Your RAG is trivial — 6 markdown files." Response?
278. "DuckDB isn't production." Response?
279. "Why not Snowflake Cortex instead?"
280. "Your router adds latency for no gain." Response?
281. "Keyword routing would be enough." Response?
282. "You don't have real tests." Response?
283. "Config doesn't match code — sloppy engineering." Response?
284. "No hybrid search in 2026?" Response?
285. "LLM still hallucinates numbers — prove grounding works."
286. "Show me failure when classification is wrong."
287. "This doesn't scale past your laptop."
288. "Medallion is overkill for 100k orders."
289. "Why Spark and not pandas?"
290. "Chroma will corrupt at scale."
291. "Your semantic layer is hardcoded YAML — not scalable."
292. "What's the novel contribution here?"
293. "I don't believe you built this — explain dynamic_sql_builder line by line."
294. "If I delete gold.executive_summary Parquet, what happens?"
295. "Sell me this architecture to a skeptical staff engineer."

---

## 9. Detailed Answer Guidance (Critical Questions)

### Q1: Does the LLM generate SQL in LakeMind?

**Testing:** Whether you understand the governance model vs text-to-SQL hype.

**Strong answer must include:** LLM classifies intent and picks route name; SQL is pre-approved in YAML or built by `dynamic_sql_builder.py` from entities; `answer_service.py` explicitly states LLM only summarizes context.

**Key concepts:** Governed semantic layer, separation of planning vs execution, hallucination risk in text-to-SQL.

**Project evidence:** `routing_service.py` lines 3-6; `answer_service.py` lines 3-4; `semantic_catalog.yaml` routes with embedded SQL.

**Weak answer:** "Yes, the LLM converts natural language to SQL using the semantic catalog."

**Strong interview answer:** "No. Ollama returns JSON with a route name like `category_performance`, and we execute the SQL already defined in `semantic_catalog.yaml`. For filtered questions, `build_dynamic_sql()` constructs SQL from extracted entities — years, states, categories — using templates, not LLM-generated strings. The answer LLM never sees write access and never emits SQL."

**Follow-ups:** What if route_name is invalid? How do you add a new metric? Why not Text-to-SQL?

---

### Q2: Walk through POST /answer for a hybrid question.

**Testing:** End-to-end ownership.

**Strong answer:** Trace through `answer_question()` → internal `ask_question()` → `route_question()` → parallel structured + retrieval paths → `compose_answer()` → chart/sources.

**Evidence:** `main.py` lines 371-424, `question_router.py` full flow.

**Weak answer:** "It goes to the LLM and RAG."

**Strong answer:** "`/answer` first calls the same logic as `/ask`. The router uses Ollama JSON classification plus entity extraction. For hybrid, we set `needs_document_retrieval=True` and still resolve structured SQL via catalog or dynamic builder. DuckDB executes read-only SQL against Parquet views. Separately, MiniLM embeds the question and Chroma returns top-k chunks. Then `build_prompt()` combines up to 20 structured rows and 5 doc chunks with grounding rules, and Ollama phi3.5 at temperature 0.1 generates prose. We also return chart metadata from rule-based `recommend_chart()`, not the LLM."

**Follow-ups:** Latency breakdown? Failure at each step? Why call ask internally instead of refactoring?

---

### Q3: Why DuckDB instead of PostgreSQL or Spark SQL for queries?

**Testing:** OLAP embeddable engine vs operational DB vs distributed compute.

**Strong answer:** Parquet-native, zero admin, in-process embeddable, perfect for local demo and portfolio; reads same files Spark writes; not for concurrent writes or multi-user warehouse scale.

**Evidence:** `duckdb_service.py` `read_parquet` views; README key decisions table.

**Weak answer:** "DuckDB is faster than everything."

**Follow-ups:** When would you switch to Snowflake/Databricks SQL? What about DuckDB persistent file?

---

### Q4: Why ChromaDB? Why not FAISS/pgvector/Pinecone?

**Testing:** Vector store trade-offs at demo vs production scale.

**Strong answer:** Local persistent store, simple Python API, metadata storage, good for portfolio; collection is tiny (~50 chunks); at millions of vectors would consider Milvus/Qdrant/pgvector with ops team.

**Evidence:** `document_ingestion.py` PersistentClient, `data/chromadb/chroma.sqlite3`.

**Follow-ups:** Migration plan? Metadata filtering? Backup strategy?

---

### Q5: Why all-MiniLM-L6-v2?

**Testing:** Embedding model selection for English business text at small scale.

**Strong answer:** 384-dim, fast on CPU, well-known baseline, sentence-transformers ecosystem; normalized embeddings for cosine similarity; would re-embed entire corpus on model change.

**Evidence:** `config.yaml` embedding.model_name; `encode(..., normalize_embeddings=True)`.

**Weak answer:** "It's the best embedding model."

**Follow-ups:** Multilingual Olist data? bge-small vs MiniLM? Evaluation method?

---

### Q6: Explain your chunking strategy.

**Testing:** RAG quality fundamentals.

**Strong answer:** Structure-aware, not fixed token size; split on `##` headers, FAQ on `**Q:` patterns, business units on `---`; enriches metadata with category/state regex; stable SHA-256 IDs; config chunk_size 500 is unused.

**Evidence:** `parse_markdown_sections`, `parse_faq`, `parse_business_unit_overviews`.

**Weak answer:** "We use 500 tokens with 50 overlap from config."

**Follow-ups:** Split mid-table problem? Add overlap? Parent-child chunks?

---

### Q7: What happens when Ollama is down?

**Testing:** Resilience and fallback paths.

**Strong answer:** Routing catches exception, falls back to keyword document signals + entity extraction + `select_semantic_route()`; `/answer` returns 503 if compose fails; `/ask` may still return structured data without LLM prose.

**Evidence:** `question_router.py` lines 38-43, 58-66; `call_ollama` RuntimeError.

**Follow-ups:** Should chat default be different? Circuit breaker design?

---

### Q8: How do you prevent malicious SQL?

**Testing:** Security awareness despite read-only guard.

**Strong answer:** Blocklist keywords (INSERT, DROP, etc.); query must start with SELECT/WITH; still need table allowlists in production; LLM doesn't write SQL; parameterized dynamic SQL from entities.

**Evidence:** `duckdb_service.py` BLOCKED_KEYWORDS, READ_ONLY_PREFIXES.

**Follow-ups:** ATTACH attack? PRAGMA bypass? Row-level security?

---

### Q9: What is allocated_payment_value?

**Testing:** Data modeling depth for revenue questions.

**Strong answer:** Gold layer allocates order-level payment across line items to avoid double-counting when summing item revenue; defined in gold transformations; exposed in METRIC_DEFINITIONS for LLM grounding.

**Evidence:** `answer_service.py` METRIC_DEFINITIONS; gold_transformations (study file).

**Follow-ups:** Show SQL for category revenue. What if analyst sums wrong column?

---

### Q10: Is this an agentic AI system?

**Testing:** Hype vs architecture clarity.

**Strong answer:** No — fixed pipeline: classify → retrieve/query → compose. No tool loop, no planner, no memory. Deliberate for predictability and interview-defensible governance.

**Follow-ups:** When would you add agents? MCP tool design?

---

*(Apply same template to remaining bank questions during study — evidence always from code paths listed in Section 3.)*

---

## 10. Adversarial Interview Chains

### Chain A — RAG

**I:** Explain your RAG architecture.  
**You:** [6 synthetic docs → section chunking → MiniLM → Chroma → dense top-k → grounded Ollama prompt]

**I:** Why vector search?  
**You:** Semantic match on policy/FAQ paraphrases; keywords miss "return window" vs "refund policy".

**I:** Why not BM25?  
**You:** Demo simplicity; corpus is tiny; dense works; production would add BM25 for exact policy IDs.

**I:** Why not hybrid?  
**You:** Honest gap — `document_retrieval_eval.py` is manual; hybrid is first prod improvement.

**I:** 100M chunks?  
**You:** Chroma breaks first; embedding reload per query breaks; need dedicated vector DB, batch index, cached model, async workers.

**I:** Migration without downtime?  
**You:** Dual-write new index, shadow compare retrieval, flip feature flag, deprecate old collection.

### Chain B — SQL / Semantic Layer

**I:** How does NL become SQL?  
**You:** It doesn't via LLM — governed routes + entity templates.

**I:** Why not let the LLM write SQL with schema in prompt?  
**You:** Hallucinated JOINs, metric inconsistency, injection risk; YAML routes encode approved business logic.

**I:** How do you add "margin by category"?  
**You:** Add Gold table or extend route SQL, register Parquet view, add catalog route + keywords, update METRIC_DEFINITIONS.

**I:** Hardcoded YAML doesn't scale.  
**You:** Agree — production moves to metric store/dbt semantic layer with CI tests on compiled SQL.

### Chain C — Architecture

**I:** 30-second pitch.  
**You:** [Section 6]

**I:** What's the weakest part?  
**You:** Embedding model reload, no retrieval eval metrics, config drift, sync blocking Ollama calls.

**I:** Redesign for production in 5 minutes.  
**You:** Containerized API, cached embeddings, pgvector/Milvus, hybrid retrieval, OpenTelemetry, auth gateway, file-backed DuckDB or warehouse, CI on routes.

---

## 11. Do NOT Bluff

| Do NOT claim | Reality |
|--------------|---------|
| "LLM generates SQL" | Governed YAML + dynamic_sql_builder only |
| "We use LangChain" | Dependency unused in src |
| "RAG module is in src/rag/" | Empty package; logic in document_ingestion.py |
| "Chunk size 500 / overlap 50" | Config unused; section-based chunking |
| "Collection rag_documents" | Hardcoded `olist_demo_documents` |
| "Local HuggingFace phi-3-mini" | Runtime Ollama phi3.5 via HTTP |
| "Hybrid BM25 + dense retrieval" | Dense only |
| "Reranking step" | Not implemented |
| "Metadata-filtered retrieval" | Metadata stored but not queried with filters |
| "Full RAG eval with MRR/NDCG" | Manual script only |
| "Dockerized full stack" | Airflow only; Spark on host |
| "Multi-turn chat memory" | Stateless per request |
| "Authentication / rate limiting" | Not implemented |
| "Incremental medallion ingest" | Overwrite mode |
| "Agents with tool use" | Fixed pipeline |
| "NL-to-SQL with schema prompt" | Explicitly avoided |
| "Real Olist company documents" | Synthetic demo docs |
| "DuckDB persistent querylake.duckdb in API" | Uses in-memory connection |
| "Automated CI test suite" | Scripts in scripts/ only |
| "Embedding model singleton cache" | Reloads each retrieval call |

---

## 12. Project Defense Cheat Sheet

| Topic | One-liner |
|-------|-----------|
| **Stack** | Spark 3.5, Parquet, DuckDB 1.1, Chroma 0.5, MiniLM, Ollama phi3.5, FastAPI, Streamlit |
| **Medallion** | Bronze raw → Silver typed → Gold 18 KPI tables |
| **RAG flow** | Markdown → section chunks → MiniLM → Chroma → dense top-k → prompt context |
| **Structured flow** | Route → catalog/dynamic SQL → DuckDB Parquet views → rows to prompt |
| **Routing** | Ollama JSON + keyword/entity fallback → chat/structured/document/hybrid |
| **LLM role** | Classify + summarize only; never define metrics or SQL |
| **Grounding** | Authoritative facts block, copy numbers exactly, cite sources |
| **Chart** | Rule-based `recommend_chart()` by table name |
| **Trade-off** | Correctness & local demo > scale & retrieval sophistication |
| **Limitations** | Tiny corpus, no hybrid/rerank, sync API, config drift, no auth |
| **Prod improvements** | Model cache, hybrid search, reranker, OTel, auth, warehouse, eval harness |

---

## 13. Study Plan (12 Days)

| Day | Focus | Actions |
|-----|-------|---------|
| **1** | Architecture + pitch | Trace diagram; memorize Sections 6-7; read architecture guide |
| **2** | DE pipeline | Run Bronze→Gold; study csv_ingestion, silver/gold transforms |
| **3** | DuckDB + semantic | Study duckdb_service, semantic_catalog.yaml, sql_builder |
| **4** | Dynamic SQL + entities | Trace dynamic_sql_builder, business_entities.yaml |
| **5** | Document ingestion | Run document_ingestion.py; understand chunk parsers |
| **6** | Embeddings + Chroma | Experiment with top_k; read Chroma persistence |
| **7** | Router | Trace question_router + routing_service; test Ollama down |
| **8** | Answer composition | Study prompts in answer_service; test grounding |
| **9** | API + UI | Hit all endpoints with curl; use chatbot_ui |
| **10** | Failures + security | Practice Chain A-C; SQL guard explanation |
| **11** | Scale + prod | Answer questions 236-255 without notes |
| **12** | Mock interview | Section 14 timed 60 min |

---

## 14. Final Mock Interview (30 Questions)

> Answer each aloud first, then check **Evaluation criteria**.

### Round 1 — Warmup (Q1-8)

**Q1.** What is LakeMind in one sentence?  
<details><summary>Evaluation criteria</summary>Olist lakehouse + governed SQL + Chroma RAG + Ollama; local portfolio platform; NOT generic chatbot.</details>

**Q2.** What datasets and documents do you use?  
<details><summary>Evaluation criteria</summary>9 Olist CSVs (~99k orders); 6 synthetic markdown docs in Master/Olist documents.</details>

**Q3.** Name the FastAPI endpoints that matter for Q&A.  
<details><summary>Evaluation criteria</summary>/ask (no LLM prose), /answer (full pipeline), /query (raw SQL), /retrieve-docs.</details>

**Q4.** What are the four question types?  
<details><summary>Evaluation criteria</summary>chat, structured, document, hybrid — from routing_service ALLOWED_QUESTION_TYPES.</details>

**Q5.** Does the LLM write SQL?  
<details><summary>Evaluation criteria</summary>No — must mention YAML routes + dynamic_sql_builder.</details>

**Q6.** What embedding model and dimension?  
<details><summary>Evaluation criteria</summary>all-MiniLM-L6-v2, 384-dim, normalize_embeddings=True.</details>

**Q7.** What LLM runs at inference?  
<details><summary>Evaluation criteria</summary>Ollama phi3.5 default; config phi-3-mini is stale.</details>

**Q8.** What's in the Gold layer?  
<details><summary>Evaluation criteria</summary>18 analytical tables including executive_summary, category_performance, geo_sales.</details>

### Round 2 — Implementation (Q9-16)

**Q9.** Trace `/answer` for "top category by revenue."  
<details><summary>Evaluation criteria</summary>Full path: router → category_performance route → DuckDB → optional no docs → compose_answer.</details>

**Q10.** How are documents chunked?  
<details><summary>Evaluation criteria</summary>Section/FAQ/BU parsers; NOT config chunk_size; SHA-256 IDs.</details>

**Q11.** What happens if Ollama fails during routing?  
<details><summary>Evaluation criteria</summary>Exception caught; keyword + entity fallback; notes in response.</details>

**Q12.** How is SQL injection prevented?  
<details><summary>Evaluation criteria</summary>Read-only keyword blocklist; LLM doesn't write SQL; mention production allowlist gap.</details>

**Q13.** Explain `allocated_payment_value`.  
<details><summary>Evaluation criteria</summary>Prevent double-counting item revenue; Gold transformation concept.</details>

**Q14.** Why DuckDB over Postgres?  
<details><summary>Evaluation criteria</summary>Parquet-native, embeddable, zero ops for local analytics demo.</details>

**Q15.** Where is RAG code — src/rag/?  
<details><summary>Evaluation criteria</summary>No — document_ingestion.py; src/rag empty.</details>

**Q16.** What's the Chroma collection name?  
<details><summary>Evaluation criteria</summary>olist_demo_documents hardcoded, not config rag_documents.</details>

### Round 3 — Deep Dive (Q17-24)

**Q17.** Why governed semantic layer vs text-to-SQL?  
<details><summary>Evaluation criteria</summary>Hallucination, metric consistency, security, debuggability.</details>

**Q18.** How would you add hybrid retrieval?  
<details><summary>Evaluation criteria</summary>BM25 index + RRF fusion before prompt; changes in retrieve_relevant_docs.</details>

**Q19.** What breaks first at 1M documents?  
<details><summary>Evaluation criteria</summary>Embedding reload, Chroma scale, sync Ollama, no batching.</details>

**Q20.** How evaluate RAG today vs production?  
<details><summary>Evaluation criteria</summary>document_retrieval_eval.py manual; prod needs Hit@K, faithfulness, golden set.</details>

**Q21.** Config vs runtime mismatches?  
<details><summary>Evaluation criteria</summary>At least 3: phi-3 vs phi3.5, chunk_size, collection name, duckdb path.</details>

**Q22.** Prompt injection risk?  
<details><summary>Evaluation criteria</summary>Malicious doc text in corpus; user question steering; mitigations: sanitization, allowlists, output validation.</details>

**Q23.** Is this agentic?  
<details><summary>Evaluation criteria</summary>No fixed pipeline; deliberate; describe what agent would add.</details>

**Q24.** How does dynamic SQL differ from catalog routes?  
<details><summary>Evaluation criteria</summary>Entities trigger templated WHERE/LIMIT; catalog is static pre-written SQL.</details>

### Round 4 — Adversarial (Q25-30)

**Q25.** "This is just ChatGPT on CSVs."  
<details><summary>Evaluation criteria</summary>Medallion DE, governed metrics, dual-path routing, grounding rules — defensible engineering.</details>

**Q26.** "Your RAG is toy-scale."  
<details><summary>Evaluation criteria</summary>Acknowledge 6 demo docs; explain architecture choices; prod roadmap.</details>

**Q27.** "Why Spark for 100k rows?"  
<details><summary>Evaluation criteria</summary>Portfolio DE patterns, scale path to larger data, Windows local dev; acknowledge pandas could work at current size.</details>

**Q28.** Redesign for 10k concurrent users.  
<details><summary>Evaluation criteria</summary>Async API, model serving tier, warehouse, vector DB cluster, cache, auth, queue.</details>

**Q29.** Biggest architectural weakness?  
<details><summary>Evaluation criteria</summary>Honest: embedding reload, no eval, sync blocking, config drift.</details>

**Q30.** What would you do differently if starting over?  
<details><summary>Evaluation criteria</summary>Unified config, cached embeddings, hybrid retrieval from day 1, proper tests/CI, single LLM call with structured output schema.</details>

---

## 15. Files to Study (Priority Order)

1. `src/api/main.py` — all endpoints
2. `src/qa/question_router.py` — orchestration hub
3. `src/llm/routing_service.py` + `answer_service.py` — prompts & Ollama
4. `src/ingestion/document_ingestion.py` — entire RAG path
5. `src/query/duckdb_service.py` — Parquet views & guards
6. `config/semantic_catalog.yaml` + `business_entities.yaml`
7. `src/semantic/dynamic_sql_builder.py`
8. `src/ingestion/csv_ingestion.py` + gold_transformations.py
9. `docs/lake_mind_architecture_and_interview_guide.md`
10. `scripts/test_greeting_routing.py`, `document_retrieval_eval.py`

---

*End of LakeMind Interview Preparation Document. All claims verified against `Master/project/` source as of repository inspection.*
