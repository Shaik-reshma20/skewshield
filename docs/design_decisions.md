# Design Decisions and Trade-offs

1. **Synthetic JSONL:** keeps the project self-contained and makes malformed
   records reproducible. CSV would make nested or malformed line handling less
   explicit; production ingestion may use Parquet/Avro.
2. **Stable event IDs:** generator IDs are deterministic for a fixed seed and
   sequence. The pipeline does not invent random IDs for bad records.
3. **Quarantine over silent drops:** every rejected row retains the raw text,
   source file and reason.
4. **Duplicate policy:** keep the lexicographically first raw representation
   per event ID. This is deterministic but is not a universal business rule;
   a production source should define event version or ingestion timestamp
   precedence.
5. **Salt only the hot key:** avoids replicating the entire dimension table.
6. **AQE is configurable:** AQE and manual salting can interact; benchmark both
   settings rather than assuming one is superior.
7. **Parquet outputs:** efficient columnar output with broad Spark support.
8. **Local overwrite-by-run-ID:** retry-safe for this single-process demo but
   not a transaction across all output folders.
9. **Double amount:** adequate for synthetic performance work; use decimal and
   currency metadata for financial correctness in production.
