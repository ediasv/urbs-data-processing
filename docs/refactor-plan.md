# Refactor plan: refined ingestion pipeline

## Scope and goals
- Focus on [dataprocessing/processors/refined_ingestion.py](dataprocessing/processors/refined_ingestion.py) and supporting Spark setup in [dataprocessing/processors/sparketl.py](dataprocessing/processors/sparketl.py).
- Target outcomes:
  - Reduce total runtime per day by removing Python UDF overhead and repeated shuffles.
  - Keep transformations inside Spark SQL for better optimization and GPU compatibility.
  - Reduce the number of Spark actions and jobs per run.

## Bottlenecks (review of suggested refactors)
1. Python UDFs block optimization and GPU support: `haversine` and `interpolate_timestamp` at [dataprocessing/processors/refined_ingestion.py](dataprocessing/processors/refined_ingestion.py#L8-L24).
2. Iterative loop with repeated `count()` and `union().orderBy()` at [dataprocessing/processors/refined_ingestion.py](dataprocessing/processors/refined_ingestion.py#L287-L353) triggers multiple full Spark jobs and shuffles.
3. Multiple window recomputations and repeated lead/lag calculations in the tracking stage increase shuffle cost at [dataprocessing/processors/refined_ingestion.py](dataprocessing/processors/refined_ingestion.py#L221-L280).
4. Join stages can be shuffle-heavy if small tables are not broadcast and if auto-broadcast is disabled in [dataprocessing/processors/sparketl.py](dataprocessing/processors/sparketl.py#L10-L16).
5. Date-level orchestration runs sequentially in [dataprocessing/job/refined_ingestion.py](dataprocessing/job/refined_ingestion.py#L30-L44), which wastes available cores.

## Refactor plan (separated steps)

### Refactor 1: Replace Python UDFs with Spark SQL expressions
**Why:** Python UDFs serialize data row-by-row, prevent Catalyst optimization, and cannot run on GPU.

**Steps:**
1. Remove `@F.udf` definitions for `haversine` and `interpolate_timestamp`.
2. Replace the `haversine(...)` calls with column expressions using Spark SQL functions:
   - Use `F.radians`, `F.sin`, `F.cos`, `F.atan2`, `F.sqrt`, `F.pow`.
   - Compute `distance_m = F.round(R * c, 2)` where `R = 6371000`.
3. Replace the `interpolate_timestamp(...)` UDF with a timestamp expression:
   - Use `F.unix_timestamp` on both timestamps.
   - Compute `s_ts + (s_next_ts - s_ts) / (next_seq - seq)`.
   - Convert back with `F.to_timestamp`.
4. Add a guard for `next_seq == seq` to avoid divide-by-zero:
   - Use `F.when(next_seq != seq, <interpolated>)` and `F.otherwise(None)`.

**Validation:** Compare row counts and sample outputs before/after, focusing on distance and interpolated timestamps.

---

### Refactor 2: Replace iterative union/count loop with sequence + explode
**Why:** The loop triggers multiple Spark jobs and shuffles. A vectorized expansion does the same work in one pass.

**Steps:**
1. Compute `next_seq` and `next_event_timestamp` once using a single window spec.
2. Build an array of missing sequence values using `F.sequence(F.col("seq") + 1, F.col("next_seq") - 1)`.
3. Use `F.explode` (or `F.posexplode`) to generate a row per missing sequence value.
4. Compute interpolated timestamps for exploded rows using the vectorized formula from Refactor 1.
5. Mark exploded rows with `generated = True`, keep original rows as `generated = False`.
6. Union the generated rows with the original rows once, then continue to the join step.

**Validation:** Ensure that the number of generated rows matches the original loop output for a sample day.

---

### Refactor 3: Consolidate window computations
**Why:** Recomputing lead/lag and row numbers increases shuffle and stage time.

**Steps:**
1. Define `windowSpec = Window.partitionBy("line_code", "vehicle", "itinerary_id").orderBy("event_timestamp")` once.
2. Add `row_num`, `next_seq_1`, `next_seq_2`, and any needed `lag` columns in a single `withColumn` chain.
3. Apply all filters using these precomputed columns.
4. Avoid extra `orderBy` calls unless required for correctness.

**Validation:** Confirm that filter logic yields the same seq progression and end-point inclusion.

---

### Refactor 4: Optimize joins and shuffles
**Why:** Join stages can be the largest shuffle costs in the pipeline.

**Steps:**
1. If `dim_bus_stop` or `bus_stop_itineraries` is small, use `broadcast(...)` on the small side.
2. If both sides are large, repartition on the join key (`line_code`, `id`) before joining.
3. Re-enable auto-broadcast joins by removing `.set('spark.sql.autoBroadcastJoinThreshold', '-1')` or setting it to a reasonable size.

**Validation:** Check the Spark UI for join strategies and reduced shuffle size.

---

### Refactor 5: Avoid `count()` for emptiness checks
**Why:** `count()` triggers full scans; `head(1)` or `take(1)` triggers minimal work.

**Steps:**
1. Replace `interpolated_points.count() == 0` with `len(interpolated_points.head(1)) == 0`.
2. If the loop is removed (Refactor 2), remove these checks entirely.

**Validation:** Measure number of Spark jobs per run (should drop).

---

### Refactor 6: Parallelize per-day execution
**Why:** Sequential day-by-day processing limits CPU usage.

**Steps:**
1. Option A: Run multiple day ranges in parallel with multiple processes (each process has its own Spark session).
2. Option B: Modify the pipeline to process multiple days in a single Spark job and partition by date columns.
3. Keep outputs partitioned by `year`, `month`, `day` to avoid file conflicts.

**Validation:** Compare wall-clock time when running multiple days.

---

### Refactor 7: Tune Spark configuration and partitioning
**Why:** Local-mode settings can be ineffective or misapplied.

**Steps:**
1. Set `spark.sql.shuffle.partitions` to a number close to CPU cores for local runs (or higher for cluster runs).
2. Use `repartition("line_code")` before heavy window or join stages to spread work evenly.
3. If moving to a cluster, replace `local[*]` with a proper master and set executor counts and memory.

**Validation:** Use Spark UI stage time and task distribution to confirm improvements.

---

### Refactor 8: Optional GPU acceleration (RAPIDS)
**Why:** If you have an NVIDIA GPU, RAPIDS can accelerate Spark SQL operations.

**Steps:**
1. Ensure all heavy transformations are Spark SQL expressions (no Python UDFs).
2. Install and configure the RAPIDS Accelerator for Apache Spark in the runtime environment.
3. Run the RAPIDS plugin to see which operations are accelerated and which fall back.

**Validation:** Compare Spark UI GPU utilization and runtime before/after.

## Suggested execution order
1. Refactor 1 (remove UDFs).
2. Refactor 2 (vectorized expansion).
3. Refactor 3 (window consolidation).
4. Refactor 4 (join optimization + broadcast).
5. Refactor 5 (remove count checks).
6. Refactor 6 and 7 (parallelism + tuning).
7. Refactor 8 (GPU, optional).

## Success criteria
- Fewer Spark jobs per run and shorter job stages.
- Reduced shuffle read/write size in Spark UI.
- Identical output schema and row counts for a sample day.
- Runtime reduction for the tracking job (largest stage).
