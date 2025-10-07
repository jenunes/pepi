# AI-Powered Index Advisor

Pepi includes an intelligent index recommendation system that analyzes your MongoDB queries and suggests optimal indexes.

## Features

✅ **Rule-Based Analysis** - Fast, accurate recommendations using proven indexing principles  
✅ **ESR Rule Implementation** - Follows MongoDB's Equality-Sort-Range best practices  
✅ **Priority Scoring** - Focus on high-impact optimizations first  
✅ **Seamless Integration** - Works out of the box, no configuration needed  
✅ **Optional AI Enhancement** - Add local LLM for advanced insights (fully optional)

## Usage

### Web UI (Recommended)

1. Upload your MongoDB log file
2. Click "Analyze Queries" to see query patterns
3. Click **"Get Index Recommendations"** button
4. View prioritized recommendations with ready-to-use index commands
5. Copy index creation commands with one click

### CLI

```bash
pepi --fetch /path/to/mongod.log --queries
# Recommendations are automatically shown for queries with COLLSCAN or high execution times
```

## How It Works

### 1. **Query Analysis**
- Parses query patterns from MongoDB logs
- Extracts execution statistics (count, duration, 95th percentile)
- Identifies queries using COLLSCAN (full collection scan)

### 2. **Smart Recommendations**
- **Critical Priority**: COLLSCAN queries executed frequently with high latency
- **High Priority**: Slow queries (>100ms) with significant execution count  
- **Medium Priority**: Moderately slow queries (>50ms) with high frequency
- **Low Priority**: Other potential optimizations

### 3. **Index Generation**
- Applies **ESR Rule** (Equality, Sort, Range) for compound indexes
- Detects text search patterns and recommends text indexes
- Generates ready-to-use `db.collection.createIndex()` commands

## Optional: AI Enhancement

For even better recommendations, you can add a local LLM (completely optional):

### Setup

```bash
# 1. Install llama-cpp-python
pip install llama-cpp-python

# 2. Download a tiny model (350MB-2GB)
python scripts/download_model.py

# 3. That's it! Pepi will automatically use it
```

### Recommended Models

- **qwen2.5-0.5b** (352MB) - Fastest, good for quick tips
- **tinyllama-1.1b** (669MB) - Balanced speed and quality
- **phi3-mini** (2.3GB) - Best quality recommendations

### Privacy

- ✅ 100% local - no data sent to external servers
- ✅ Runs on CPU - no GPU required
- ✅ Optional - works great without LLM too
- ✅ No API keys or internet needed

## Example Output

```
🔴 CRITICAL Priority
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Namespace: mydb.users
Operation: find
Pattern: {"age": {"$gt": "?"}, "status": "?"}

Current Index: COLLSCAN
Executed: 150× | Avg: 234ms | P95: 450ms

Recommended Index:
db.users.createIndex({"status": 1, "age": 1})

Why: Query performs full collection scan. Filters on status 
and range on age. Executed 150× averaging 234ms. An index 
enables efficient B-tree lookup instead of scanning all documents.

Estimated Improvement: 80-95% faster (estimated 10-40ms)

💡 AI Tip: Consider adding status as first field since it's
an equality match, providing better selectivity.
```

## Technical Details

### Priority Calculation

```
priority_score = (count × mean_duration_ms) / 1000

Multipliers:
- Has COLLSCAN: ×2
- Mean > 200ms: ×1.5  
- Count > 100: ×1.3
```

### ESR Rule Application

1. **Equality** fields first (exact matches)
2. **Sort** fields second (if present)
3. **Range** fields last ($gt, $lt, $in, etc.)

This order provides optimal index usage for most queries.

### Field Detection

- Regular expressions parse query patterns
- Extracts field names and usage types (equality/range/sort/text)
- Handles complex aggregation pipelines
- Works with find, update, delete, and aggregate operations

## Limitations

- Requires JSON-formatted MongoDB logs (v4.4+)
- Cannot detect application-level query patterns
- Index recommendations assume typical workload patterns
- Some complex queries may need manual tuning

## FAQ

**Q: Do I need to install anything extra?**  
A: No! Works out of the box with rule-based recommendations.

**Q: Is the LLM required?**  
A: No, it's completely optional. The rule-based system is very effective on its own.

**Q: How much does the LLM cost?**  
A: It's free! Uses open-source models that run locally.

**Q: Will it slow down my analysis?**  
A: No, recommendations are generated only when you click the button.

**Q: Can I use it without internet?**  
A: Yes! Everything runs locally (after initial model download if using LLM).

**Q: How accurate are the recommendations?**  
A: Very accurate for common patterns. Always test indexes in a non-production environment first.

## Next Steps

1. Try it with your MongoDB logs
2. Review recommendations sorted by priority
3. Test suggested indexes in a development environment
4. Monitor performance improvements
5. Optionally add LLM for enhanced insights

Happy optimizing! 🚀

