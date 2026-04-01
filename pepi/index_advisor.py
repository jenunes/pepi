"""
Rule-based index recommendations for MongoDB queries.
Provides accurate, fast analysis without AI dependencies.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# MongoDB Official Documentation - ESR (Equality, Sort, Range) Guideline
# Source: https://www.mongodb.com/docs/manual/tutorial/equality-sort-range-guideline/
MONGODB_ESR_GUIDELINES = """
ESR (Equality, Sort, Range) Guideline for MongoDB Compound Indexes:

An index that references multiple fields is a compound index.
Index keys correspond to document fields.

KEY PRINCIPLES:
1. EQUALITY fields must ALWAYS come first
   - Exact matches on single values (e.g., field: "value" or field: {$eq: "value"})
   - Equality matches are most selective and reduce the search space dramatically
   - Multiple equality fields can appear in any order (but all must be before sort/range)
   - More selective equality matches = more efficient queries

2. SORT fields come second (when avoiding in-memory sorts is critical)
   - Determines result ordering
   - When query fields are a subset of index keys, MongoDB can use the index for sorting
   - Avoids expensive in-memory SORT operations
   - Must match the sort direction in the query
   - IMPORTANT: If your range predicate is very selective, consider ERS (put range before sort)

3. RANGE fields come last
   - Filters that scan without exact match (loosely bound to index keys)
   - Examples: {$gt, $lt, $gte, $lte, $ne, $nin, $regex}
   - Less selective than equality, more scanning required
   - Limit range bounds when possible

SPECIAL CASES:
- $in operator:
  * With < 201 elements: Acts like equality (uses SORT_MERGE stage)
  * With >= 201 elements: Acts like range operator
  * For small arrays, include $in early in index; for large arrays, treat as range
  
- $ne and $nin are RANGE operators, not equality
- $regex is a RANGE operator

INDEX SORT ORDER:
- Ascending (1) vs Descending (-1) matters for multi-field sorts
- For single-direction sorts, index order can be reversed
- For mixed-direction sorts (e.g., {a: 1, b: -1}), index must match exactly

PERFORMANCE BENEFITS:
- B-tree traversal: O(log n) instead of O(n) collection scan
- Index bounds: Skip directly to matching documents
- In-index sorting: Eliminate expensive in-memory sorts
- Reduced document examination: Only scan relevant index entries

QUERY PLAN IMPROVEMENTS:
- COLLSCAN → IXSCAN: Use index instead of full collection scan
- Remove SORT stage: Results already ordered from index
- Better keysExamined:docsExamined ratio (ideally close to 1:1)
- Index-only scans possible when all fields are in index (covered queries)
"""

# MongoDB Index Strategies - Additional Context
# Source: https://www.mongodb.com/docs/manual/tutorial/sort-results-with-indexes/
# Source: https://www.mongodb.com/docs/manual/tutorial/create-queries-that-ensure-selectivity/
MONGODB_INDEX_STRATEGIES = """
SORT WITH INDEXES:
- Index can support sort when query fields are subset of index keys
- Sort on non-prefix subset only works if query has equality conditions on all prefix keys
- Compound index {a: 1, b: 1} supports: sort({a: 1}), sort({a: 1, b: 1}), sort({b: 1})
  with equality on 'a'
- Cannot use index for sort if: skip() is used, sort fields aren't in index, or sort order conflicts

ENSURING SELECTIVITY:
- Selective indexes examine fewer documents per query
- Create indexes on fields that appear frequently in queries
- Avoid indexes on fields with low cardinality (few unique values)
- Use compound indexes to target specific query patterns
- Index prefix must match query for index to be effective
- More selective fields should come first in compound indexes

AGGREGATION PIPELINE CONSIDERATIONS:
- $match stages benefit from indexes (especially early in pipeline)
- $sort stages can use indexes to avoid in-memory sorting
- $lookup may benefit from indexes on foreign collection
- Index on fields used in both $match and $sort (ESR applies)
- Early $match + $sort can be optimized if index covers both operations
"""


def _pattern_for_advisor(stats: Dict, fallback_pattern: str) -> str:
    """Use full representative command JSON for field extraction when available."""
    rc = stats.get("repr_command")
    if rc is not None:
        try:
            return json.dumps(rc)
        except (TypeError, ValueError):
            pass
    return fallback_pattern


def analyze_queries(query_stats: Dict) -> List[Dict]:
    """Analyze queries and generate index recommendations.

    Args:
        query_stats: Dictionary of query statistics from calculate_query_stats

    Returns:
        List of recommendations sorted by priority
    """
    recommendations = []

    for (namespace, operation, pattern), stats in query_stats.items():
        if _is_system_collection(namespace):
            continue

        if not _needs_index(stats):
            continue

        priority = _calculate_priority(stats)

        advisor_pattern = _pattern_for_advisor(stats, pattern)
        rec = _generate_recommendation(namespace, operation, advisor_pattern, stats)

        if rec:
            rec["priority"] = priority
            rec["priority_level"] = _get_priority_level(priority)
            recommendations.append(rec)

    recommendations.sort(key=lambda x: x["priority"], reverse=True)

    return recommendations


def analyze_single_query(
    namespace: str, operation: str, pattern: str, stats: Dict
) -> Optional[Dict]:
    """Analyze a single query for detailed recommendations.

    When user explicitly clicks "Get Index Recommendation", analyze the query
    but skip if the index is already optimal to prevent unnecessary recommendations.

    Args:
        namespace: Query namespace (db.collection)
        operation: Query operation (find, aggregate, etc.)
        pattern: Query pattern
        stats: Query statistics

    Returns:
        Single recommendation dict or None
    """
    logger.info("analyze_single_query namespace=%s operation=%s", namespace, operation)
    logger.debug("stats=%s", stats)
    logger.debug("indexes=%s", stats.get("indexes", "NOT FOUND"))

    if _is_system_collection(namespace):
        logger.warning("Skipping system collection namespace=%s", namespace)
        return None

    fields = _extract_query_fields(pattern, operation)
    if not fields:
        logger.warning("No fields detected for query pattern")
        return None

    current_index_info = _get_current_index_info(stats)
    coverage_analysis = _analyze_index_coverage(fields, current_index_info, stats)

    if (
        coverage_analysis["recommendation_type"] == "OPTIMIZED"
        and coverage_analysis["coverage_score"] >= 90
    ):
        logger.info(
            "Query is already optimally indexed (coverage=%s), skipping analysis",
            coverage_analysis["coverage_score"],
        )
        return None

    logger.info("Analyzing user-requested query recommendation")

    priority = _calculate_priority(stats)
    logger.debug("priority_score=%s", priority)

    rec = _generate_recommendation(namespace, operation, pattern, stats)
    logger.debug("generated_recommendation=%s", rec is not None)
    if rec:
        logger.debug("recommendation_keys=%s", list(rec.keys()))
        rec["priority"] = priority
        rec["priority_level"] = _get_priority_level(priority)

    return rec


def _is_system_collection(namespace: str) -> bool:
    """Check if namespace is a system collection that shouldn't be touched."""
    if not namespace or "." not in namespace:
        return False

    db, collection = namespace.split(".", 1)

    system_dbs = ["admin", "config", "local"]
    if db in system_dbs:
        return True

    if collection.startswith("system."):
        return True

    return False


def _needs_index(stats: Dict) -> bool:
    """Check if query needs index improvement."""
    indexes = stats.get("indexes", set())

    if isinstance(indexes, list):
        indexes = set(indexes)

    if "COLLSCAN" in indexes:
        return True

    if stats.get("mean", 0) > 100 and stats.get("count", 0) > 5:
        return True

    if stats.get("count", 0) > 50 and stats.get("mean", 0) > 50:
        return True

    avg_docs = stats.get("avg_docs_examined")
    avg_nr = stats.get("avg_n_returned")
    if avg_docs is not None and avg_nr is not None and avg_nr > 0:
        if (avg_docs / avg_nr) > 100:
            return True

    return False


def _calculate_priority(stats: Dict) -> float:
    """Calculate priority score (0-1000) based on impact."""
    count = stats.get("count", 0)
    mean_ms = stats.get("mean", 0)
    indexes = stats.get("indexes", set())

    if isinstance(indexes, list):
        indexes = set(indexes)

    has_collscan = "COLLSCAN" in indexes

    base_score = count * mean_ms / 1000

    if has_collscan and base_score < 10:
        base_score = 10

    if has_collscan:
        base_score *= 2

    if mean_ms > 200:
        base_score *= 1.5

    if count > 100:
        base_score *= 1.3

    avg_docs = stats.get("avg_docs_examined")
    avg_nr = stats.get("avg_n_returned")
    if avg_docs is not None and avg_nr is not None and avg_nr > 0:
        ratio = avg_docs / avg_nr
        if ratio > 50:
            base_score *= 1.2
        if ratio > 200:
            base_score *= 1.15

    return min(base_score, 1000)


def _get_priority_level(score: float) -> str:
    """Convert priority score to level."""
    if score >= 100:
        return "CRITICAL"
    elif score >= 50:
        return "HIGH"
    elif score >= 20:
        return "MEDIUM"
    else:
        return "LOW"


def _placeholder_explain_values(obj: Any) -> Any:
    """Replace scalars with placeholders for safe explain command snippets."""
    if isinstance(obj, dict):
        return {k: _placeholder_explain_values(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_placeholder_explain_values(v) for v in obj]
    return "<value>"


def _generate_explain_command(namespace: str, operation: str, pattern: str) -> str:
    """Build a mongosh explain('executionStats') skeleton from the command pattern."""
    db, coll = namespace.split(".", 1) if "." in namespace else ("db", namespace)
    base = f'db.getSiblingDB("{db}").getCollection("{coll}")'
    try:
        cmd = json.loads(pattern)
    except (json.JSONDecodeError, TypeError):
        return f'{base}.find({{}}).explain("executionStats")  // Unparseable command; edit filter'

    if operation == "find":
        if isinstance(cmd, dict) and "find" in cmd:
            filt = _placeholder_explain_values(cmd.get("filter", {}))
            parts: List[str] = [f"{base}.find({json.dumps(filt)})"]
            sort = cmd.get("sort")
            if sort and isinstance(sort, dict):
                parts.append(f".sort({json.dumps(_placeholder_explain_values(sort))})")
            if cmd.get("skip") is not None:
                parts.append(".skip(<number>)")
            if cmd.get("limit") is not None:
                parts.append(".limit(<number>)")
            return "".join(parts) + '.explain("executionStats")'
        if isinstance(cmd, dict):
            filt = _placeholder_explain_values(cmd)
            return f'{base}.find({json.dumps(filt)}).explain("executionStats")'

    if operation == "aggregate":
        pipeline: List[Any] = []
        if isinstance(cmd, dict) and "aggregate" in cmd:
            pipeline = cmd.get("pipeline", []) or []
        elif isinstance(cmd, list):
            pipeline = cmd
        if pipeline:
            ph = _placeholder_explain_values(pipeline)
            return f'{base}.explain("executionStats").aggregate({json.dumps(ph)})'
        return f'{base}.explain("executionStats").aggregate([])'

    if operation == "update" and isinstance(cmd, dict) and "updates" in cmd:
        return (
            f'{base}.explain("executionStats").updateMany('
            f"<filter>, <update>)  // Expand from command.updates"
        )
    if operation == "delete" and isinstance(cmd, dict) and "deletes" in cmd:
        return (
            f'{base}.explain("executionStats").deleteMany(<filter>)  // Expand from command.deletes'
        )

    return f'{base}.find({{}}).explain("executionStats")  // operation={operation}'


def _build_esr_breakdown(
    fields: List[Tuple[str, str]],
    index_spec: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Human-readable E/S/R classification per field for UI transparency."""
    rec_keys = [k for k in index_spec.keys()] if index_spec else []
    evidence_map = {
        "equality": "Equality predicate in filter (exact match)",
        "range": "Range or inequality operator in filter",
        "sort": "Sort clause or $sort stage in command",
        "text": "$text or regex-style predicate",
    }
    out: List[Dict[str, Any]] = []
    for field, usage in fields:
        pos: Optional[int] = None
        if field in rec_keys:
            pos = rec_keys.index(field) + 1
        out.append(
            {
                "field": field,
                "classification": usage,
                "evidence": evidence_map.get(usage, usage),
                "position_in_index": pos,
            }
        )
    return out


def _compute_suboptimal_order(
    fields: List[Tuple[str, str]],
    index_spec: Dict[str, Any],
    current_structure: List[Tuple[str, int]],
) -> List[str]:
    """Compare current index key order to ESR expectations and recommended spec."""
    issues: List[str] = []
    if not current_structure or not index_spec:
        return issues

    cur_fields = [f for f, _ in current_structure]
    rec_fields = [k for k in index_spec.keys() if index_spec[k] != "text"]

    equality_fields = list(dict.fromkeys([f for f, t in fields if t == "equality"]))
    sort_fields = list(dict.fromkeys([f for f, t in fields if t == "sort"]))
    range_fields = list(dict.fromkeys([f for f, t in fields if t == "range"]))

    def pos(field: str) -> Optional[int]:
        return cur_fields.index(field) if field in cur_fields else None

    for r in range_fields:
        for e in equality_fields:
            pr, pe = pos(r), pos(e)
            if pr is not None and pe is not None and pr < pe:
                issues.append(
                    f"Current index: range field '{r}' appears before equality field '{e}'",
                )
        for s in sort_fields:
            pr, ps = pos(r), pos(s)
            if pr is not None and ps is not None and pr < ps:
                issues.append(
                    f"Current index: range field '{r}' appears before sort field '{s}'",
                )

    rec_set = set(rec_fields)
    cur_set = set(cur_fields)
    if rec_set == cur_set and rec_fields and cur_fields and rec_fields != cur_fields:
        issues.append(
            f"Current index key order {cur_fields} differs from recommended ESR order {rec_fields}",
        )

    return issues


def _generate_recommendation(
    namespace: str, operation: str, pattern: str, stats: Dict
) -> Optional[Dict]:
    """Generate index recommendation for a query."""

    fields = _extract_query_fields(pattern, operation)

    current_index_info = _get_current_index_info(stats)

    if not fields and current_index_info["type"] == "COLLSCAN":
        return _generate_generic_collscan_recommendation(namespace, operation, pattern, stats)

    if not fields:
        return None

    coverage_analysis = _analyze_index_coverage(fields, current_index_info, stats)

    if (
        coverage_analysis["recommendation_type"] == "OPTIMIZED"
        and coverage_analysis["coverage_score"] >= 90
    ):
        logger.info(
            "Query is already optimally indexed (coverage=%s)",
            coverage_analysis["coverage_score"],
        )
        return None

    index_spec = _build_index_spec(fields, operation, pattern)

    if not index_spec:
        if current_index_info["type"] == "COLLSCAN":
            return _generate_generic_collscan_recommendation(namespace, operation, pattern, stats)
        return None

    subopts = _compute_suboptimal_order(
        fields,
        index_spec,
        current_index_info.get("structure", []),
    )
    if subopts:
        coverage_analysis["suboptimal_order"] = subopts
        for line in subopts:
            if line not in coverage_analysis["improvement_details"]:
                coverage_analysis["improvement_details"].append(line)

    query_field_types = {f: t for f, t in fields}
    selectivity = _analyze_selectivity(
        query_field_types,
        stats,
        current_index_info.get("structure", []),
    )
    for issue in selectivity.get("issues", []):
        tag = f"Selectivity: {issue}"
        if tag not in coverage_analysis["improvement_details"]:
            coverage_analysis["improvement_details"].append(tag)

    migration_strategy = _generate_migration_strategy(
        namespace, current_index_info, index_spec, coverage_analysis
    )

    reason = _generate_reason(fields, operation, stats, coverage_analysis)
    if selectivity.get("issues"):
        reason += " " + " ".join(selectivity["issues"][:2])

    display_pattern = stats.get("pattern") or pattern
    explain_cmd = _generate_explain_command(namespace, operation, pattern)
    esr_breakdown = _build_esr_breakdown(fields, index_spec)

    rec = {
        "namespace": namespace,
        "operation": operation,
        "pattern": display_pattern[:200],
        "current_index": current_index_info["type"],
        "current_index_structure": current_index_info.get("structure", []),
        "stats": {
            "count": stats.get("count", 0),
            "mean_ms": round(stats.get("mean", 0), 1),
            "p95_ms": round(stats.get("percentile_95", 0), 1),
        },
        "coverage_analysis": coverage_analysis,
        "esr_breakdown": esr_breakdown,
        "recommendation": {
            "index_spec": index_spec,
            "command": _format_create_index(namespace, index_spec),
            "reason": reason,
            "migration_strategy": migration_strategy,
            "explain_command": explain_cmd,
        },
    }

    return rec


def _get_current_index_info(stats: Dict) -> Dict:
    """Extract current index information from stats."""
    plan_summary = stats.get("plan_summary", "COLLSCAN")
    return _parse_plan_summary(plan_summary)


def _generate_migration_strategy(
    namespace: str, current_index: Dict, recommended_index: Dict, coverage_analysis: Dict
) -> Dict:
    """Generate migration strategy for index changes."""
    strategy = {
        "type": coverage_analysis["recommendation_type"],
        "commands": [],
        "warnings": [],
        "estimated_impact": "low",
    }

    rec_type = coverage_analysis["recommendation_type"]

    if rec_type == "CREATE_NEW":
        strategy["commands"].append(
            {
                "action": "create",
                "command": _format_create_index(namespace, recommended_index),
                "description": "Create new index",
            }
        )
        strategy["estimated_impact"] = "low"

    elif rec_type == "IMPROVE_EXISTING":
        strategy["warnings"].append(
            "This will replace an existing index. Monitor performance during index build."
        )
        strategy["commands"].extend(
            [
                {
                    "action": "create",
                    "command": _format_create_index(namespace, recommended_index),
                    "description": "Create improved index",
                },
                {
                    "action": "drop",
                    "command": (
                        f"// Drop old index after new one is created\n"
                        f"// db.{namespace.split('.')[-1]}.dropIndex({{ /* old index spec */ }})"
                    ),
                    "description": "Drop old index (after verifying new one works)",
                },
            ]
        )
        strategy["estimated_impact"] = "medium"

    elif rec_type == "EXTEND_INDEX":
        strategy["warnings"].append(
            "Consider if extending index is better than creating a new compound index."
        )
        strategy["commands"].append(
            {
                "action": "create",
                "command": _format_create_index(namespace, recommended_index),
                "description": "Create extended index with additional fields",
            }
        )
        strategy["estimated_impact"] = "low"

    elif rec_type == "REPLACE_INDEX":
        strategy["warnings"].append(
            "Complete index replacement required. Plan for maintenance window."
        )
        strategy["commands"].extend(
            [
                {
                    "action": "create",
                    "command": _format_create_index(namespace, recommended_index),
                    "description": "Create new optimized index",
                },
                {
                    "action": "drop",
                    "command": (
                        f"// Drop old index after verification\n"
                        f"// db.{namespace.split('.')[-1]}.dropIndex({{ /* old index spec */ }})"
                    ),
                    "description": "Drop old index (after verification)",
                },
            ]
        )
        strategy["estimated_impact"] = "high"

    elif rec_type == "OPTIMIZED":
        strategy["commands"].append(
            {
                "action": "none",
                "command": "// Index is already optimal for this query pattern",
                "description": "No changes needed",
            }
        )
        strategy["estimated_impact"] = "none"

    return strategy


def _generate_generic_collscan_recommendation(
    namespace: str, operation: str, pattern: str, stats: Dict
) -> Dict:
    """Generate generic recommendation for COLLSCAN queries where fields couldn't be extracted."""

    count = stats.get("count", 0)
    mean_ms = stats.get("mean", 0)

    reason = "Query performs full collection scan (COLLSCAN). "
    reason += f"Executed {count}× averaging {mean_ms:.0f}ms. "

    if operation == "aggregate":
        reason += (
            "This aggregation pipeline requires analysis. Review the pipeline stages "
            "($match, $sort, $group) and create indexes on fields used in $match and $sort stages."
        )
        command = (
            f"// Analyze pipeline and create index on filtered/sorted fields\n"
            f"// db.{namespace.split('.')[-1]}.createIndex({{ field: 1 }})"
        )
    else:
        reason += (
            "Query pattern is complex. Analyze which fields are frequently queried "
            "and create appropriate indexes."
        )
        command = (
            f"// Analyze query and create index on frequently used fields\n"
            f"// db.{namespace.split('.')[-1]}.createIndex({{ field: 1 }})"
        )

    disp = stats.get("pattern") or pattern
    explain_cmd = _generate_explain_command(namespace, operation, pattern)
    if selectivity_notes := _analyze_selectivity({}, stats, []).get("issues"):
        reason += " " + " ".join(selectivity_notes[:2])

    rec = {
        "namespace": namespace,
        "operation": operation,
        "pattern": disp[:200],
        "current_index": "COLLSCAN",
        "stats": {
            "count": count,
            "mean_ms": round(mean_ms, 1),
            "p95_ms": round(stats.get("percentile_95", 0), 1),
        },
        "esr_breakdown": [],
        "recommendation": {
            "index_spec": "Manual analysis required",
            "command": command,
            "reason": reason,
            "estimated_improvement": _estimate_improvement(stats),
            "explain_command": explain_cmd,
        },
    }

    return rec


def _extract_fields_from_match_clause(match_clause: dict, fields: List[Tuple[str, str]]) -> None:
    """Recursively extract fields from a $match clause, handling $and, $or, $nor.

    Args:
        match_clause: The match clause dictionary
        fields: List to append extracted fields to (modified in place)
    """
    for field_name, field_value in match_clause.items():
        if field_name in ("$and", "$or", "$nor"):
            if isinstance(field_value, list):
                for condition in field_value:
                    if isinstance(condition, dict):
                        _extract_fields_from_match_clause(condition, fields)
        elif field_name.startswith("$"):
            continue
        else:
            if isinstance(field_value, dict):
                operators = [k for k in field_value.keys() if k.startswith("$")]
                if operators:
                    range_ops = {"$gt", "$gte", "$lt", "$lte", "$ne", "$nin", "$regex", "$exists"}
                    in_op = {"$in"}
                    eq_op = {"$eq"}

                    if any(op in range_ops for op in operators):
                        fields.append((field_name, "range"))
                    elif any(op in in_op for op in operators):
                        in_value = field_value.get("$in", [])
                        if isinstance(in_value, list) and len(in_value) >= 201:
                            fields.append((field_name, "range"))
                        else:
                            fields.append((field_name, "equality"))
                    elif any(op in eq_op for op in operators):
                        fields.append((field_name, "equality"))
                    else:
                        fields.append((field_name, "range"))
                else:
                    fields.append((field_name, "equality"))
            else:
                fields.append((field_name, "equality"))


def _extract_query_fields(pattern: str, operation: str) -> List[Tuple[str, str]]:
    """Extract fields and their usage from query pattern.

    Accepts the FULL command object (including filter, sort, projection, etc.)
    instead of just normalized patterns.

    Returns:
        List of (field_name, usage_type) tuples
        usage_type: 'equality', 'range', 'sort', 'text'
    """
    fields = []

    logger.debug("Extracting fields from operation=%s pattern_prefix=%s", operation, pattern[:200])

    try:
        if operation == "find":
            try:
                query = json.loads(pattern)
                if isinstance(query, dict):
                    if "find" in query:
                        logger.debug("Detected full find command object")

                        filter_clause = query.get("filter", {})
                        if filter_clause:
                            _extract_fields_from_match_clause(filter_clause, fields)

                        sort_clause = query.get("sort", {})
                        if sort_clause and isinstance(sort_clause, dict):
                            for field_name in sort_clause.keys():
                                if not field_name.startswith("$"):
                                    fields.append((field_name, "sort"))

                        logger.debug("Extracted %d fields from full command", len(fields))
                    else:
                        _extract_fields_from_match_clause(query, fields)
                        logger.debug(
                            "JSON parsing succeeded for find, extracted %d fields", len(fields)
                        )
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning("JSON parsing failed for find (%s), falling back to regex", e)
                equality_fields = re.findall(r'"([^"]+)":\s*"[^"]*"', pattern)
                for field in equality_fields:
                    if not field.startswith("$"):
                        fields.append((field, "equality"))

                range_patterns = [
                    r'"([^"]+)":\s*\{"(\$gt|\$gte|\$lt|\$lte|\$ne)"',
                    r'"([^"]+)":\s*\{"(\$in|\$nin)"',
                ]
                for pat in range_patterns:
                    matches = re.findall(pat, pattern)
                    for field, op in matches:
                        if not field.startswith("$"):
                            fields.append((field, "range"))

                if "$text" in pattern or "$regex" in pattern:
                    text_fields = re.findall(r'"([^"]+)":\s*\{"\$regex"', pattern)
                    for field in text_fields:
                        fields.append((field, "text"))

                if '"sort"' in pattern.lower():
                    sort_fields = re.findall(r'"sort":\s*\{[^}]*"([^"]+)"', pattern)
                    for field in sort_fields:
                        if not field.startswith("$"):
                            fields.append((field, "sort"))

        elif operation == "aggregate":
            try:
                parsed = json.loads(pattern)

                if isinstance(parsed, dict) and "aggregate" in parsed:
                    logger.debug("Detected full aggregate command object")
                    pipeline = parsed.get("pipeline", [])
                elif isinstance(parsed, list):
                    pipeline = parsed
                else:
                    logger.warning("Unknown aggregate format: %s", type(parsed))
                    pipeline = []

                if isinstance(pipeline, list):
                    for stage in pipeline:
                        if isinstance(stage, dict):
                            if "$match" in stage:
                                match_clause = stage["$match"]
                                _extract_fields_from_match_clause(match_clause, fields)

                            if "$sort" in stage:
                                sort_clause = stage["$sort"]
                                for field_name in sort_clause.keys():
                                    if not field_name.startswith("$"):
                                        fields.append((field_name, "sort"))

                    logger.debug(
                        "JSON parsing succeeded for aggregate, extracted %d fields", len(fields)
                    )
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning("JSON parsing failed for aggregate: %s", e)
                match_sections = re.findall(r"\$match[,\s]*\{[^}]*\}", pattern)
                for match_section in match_sections:
                    match_fields = re.findall(r'"([^"$][^"]*)":\s*["{[]', match_section)
                    for field in match_fields:
                        if not field.startswith("$") and field not in ("$match", "$sort", "$group"):
                            fields.append((field, "equality"))

                sort_sections = re.findall(r"\$sort[,\s]*\{[^}]*\}", pattern)
                for sort_section in sort_sections:
                    sort_fields = re.findall(r'"([^"$][^"]*)":\s*[-\d]', sort_section)
                    for field in sort_fields:
                        if not field.startswith("$"):
                            fields.append((field, "sort"))

                if not fields:
                    all_fields = re.findall(r'"([a-zA-Z_][a-zA-Z0-9_\.]*)":', pattern)
                    for field in all_fields:
                        if not field.startswith("$") and field not in (
                            "$match",
                            "$sort",
                            "$group",
                            "$project",
                            "$lookup",
                            "$unwind",
                        ):
                            if (field, "equality") not in fields and (field, "sort") not in fields:
                                fields.append((field, "equality"))

        elif operation in ("update", "delete"):
            try:
                parsed = json.loads(pattern)
                if isinstance(parsed, dict):
                    if operation in parsed:
                        logger.debug("Detected full %s command object", operation)
                        items = (
                            parsed.get("updates", [])
                            if operation == "update"
                            else parsed.get("deletes", [])
                        )
                        for item in items:
                            if isinstance(item, dict) and "q" in item:
                                query_clause = item["q"]
                                if isinstance(query_clause, dict):
                                    _extract_fields_from_match_clause(query_clause, fields)
                    else:
                        query_fields = re.findall(r'"q":\s*\{[^}]*"([^"]+)":', pattern)
                        for field in query_fields:
                            if not field.startswith("$"):
                                fields.append((field, "equality"))
            except (json.JSONDecodeError, TypeError):
                query_fields = re.findall(r'"q":\s*\{[^}]*"([^"]+)":', pattern)
                for field in query_fields:
                    if not field.startswith("$"):
                        fields.append((field, "equality"))

    except Exception:
        pass

    seen = set()
    unique_fields = []
    for field, usage in fields:
        key = (field, usage)
        if key not in seen:
            seen.add(key)
            unique_fields.append(key)

    logger.debug("Final unique fields: %s", unique_fields)

    return unique_fields


def _build_index_spec(
    fields: List[Tuple[str, str]], operation: str, pattern: str
) -> Optional[Dict]:
    """Build index specification from extracted fields."""

    if not fields:
        return None

    index_spec = {}

    equality_fields = [f for f, u in fields if u == "equality"]
    sort_fields = [f for f, u in fields if u == "sort"]
    range_fields = [f for f, u in fields if u == "range"]
    text_fields = [f for f, u in fields if u == "text"]

    if text_fields:
        for field in text_fields:
            index_spec[field] = "text"
        return index_spec

    for field in equality_fields:
        index_spec[field] = 1

    for field in sort_fields:
        if field not in index_spec:
            index_spec[field] = 1

    for field in range_fields:
        if field not in index_spec:
            index_spec[field] = 1

    return index_spec if index_spec else None


def _format_create_index(namespace: str, index_spec: Dict) -> str:
    """Format MongoDB createIndex command with explicit database and collection."""
    db, collection = namespace.split(".", 1) if "." in namespace else ("db", namespace)

    spec_str = json.dumps(index_spec, indent=2)

    return f'db.getSiblingDB("{db}").getCollection("{collection}").createIndex({spec_str})'


def _generate_reason(
    fields: List[Tuple[str, str]], operation: str, stats: Dict, coverage_analysis: Dict = None
) -> str:
    """Generate human-readable reason for recommendation."""

    indexes = stats.get("indexes", set())
    if isinstance(indexes, list):
        indexes = set(indexes)

    has_collscan = "COLLSCAN" in indexes
    count = stats.get("count", 0)
    mean_ms = stats.get("mean", 0)

    field_names = [f for f, _ in fields]
    {f: t for f, t in fields}

    if coverage_analysis and coverage_analysis.get("recommendation_type") == "OPTIMIZED":
        reason = "Index structure is optimal and follows ESR principles. "
        reason += (
            f"Query slowness ({mean_ms:.0f}ms average, {count}× executions) is likely due to: "
        )
        reason += "data volume, poor selectivity, or result set size. "
        reason += (
            "Consider adding more selective filters, using limit, or investigating data patterns."
        )
        return reason

    if has_collscan:
        reason = "Query performs full collection scan. "

        equality = [f for f, t in fields if t == "equality"]
        ranges = [f for f, t in fields if t == "range"]
        sorts = [f for f, t in fields if t == "sort"]

        if equality and not ranges and not sorts:
            reason += f"Filters on {', '.join(equality)}. "
        elif equality and sorts:
            reason += f"Filters on {', '.join(equality)} and sorts by {', '.join(sorts)}. "
        elif equality and ranges:
            reason += f"Uses equality on {', '.join(equality)} and range on {', '.join(ranges)}. "
        else:
            reason += f"Uses fields: {', '.join(field_names)}. "

        reason += f"Executed {count}× averaging {mean_ms:.0f}ms. "
        reason += "An index enables efficient B-tree lookup instead of scanning all documents."
    else:
        if coverage_analysis:
            coverage_score = coverage_analysis.get("coverage_score", 0)
            esr_violations = coverage_analysis.get("esr_violations", [])
            missing_fields = coverage_analysis.get("missing_fields", [])

            if coverage_score < 50:
                reason = (
                    f"Current index has poor coverage ({coverage_score}%) for this query pattern. "
                )
                if missing_fields:
                    reason += f"Missing fields: {', '.join(missing_fields)}. "
                if esr_violations:
                    reason += f"ESR violations: {'; '.join(esr_violations[:2])}. "
            elif esr_violations:
                reason = (
                    f"Index exists but violates ESR principles: {'; '.join(esr_violations[:2])}. "
                )
                reason += f"Query executed {count}× averaging {mean_ms:.0f}ms. "
                reason += "Reordering index fields will improve performance."
            else:
                reason = (
                    f"Query is slow ({mean_ms:.0f}ms average, {count}× executions) "
                    "despite having an index. "
                )
                reason += "Current index may not be optimal for this access pattern. "
                reason += (
                    "A better-targeted compound index could significantly improve performance."
                )
        else:
            reason = f"Query is slow ({mean_ms:.0f}ms average, {count}× executions). "
            reason += "Current index may not be optimal for this access pattern. "
            reason += "A better-targeted compound index could significantly improve performance."

    ad = stats.get("avg_docs_examined")
    an = stats.get("avg_n_returned")
    if ad is not None and an is not None and an > 0 and (ad / an) > 20:
        reason += f" Avg execution: {ad:.0f} docs examined vs {an:.0f} returned ({ad / an:.0f}:1)."

    return reason


def _estimate_improvement(stats: Dict) -> str:
    """Estimate performance improvement."""

    indexes = stats.get("indexes", set())
    if isinstance(indexes, list):
        indexes = set(indexes)

    has_collscan = "COLLSCAN" in indexes
    mean_ms = stats.get("mean", 0)

    if has_collscan:
        if mean_ms > 500:
            return "90-98% faster (estimated <50ms)"
        elif mean_ms > 200:
            return "80-95% faster (estimated 10-40ms)"
        elif mean_ms > 100:
            return "70-85% faster (estimated 15-30ms)"
        else:
            return "60-80% faster"
    else:
        if mean_ms > 200:
            return "40-60% faster (estimated)"
        else:
            return "30-50% faster (estimated)"


def _parse_plan_summary(plan_summary: str) -> Dict:
    """Parse planSummary to extract index structure and type.

    Args:
        plan_summary: String like "IXSCAN { status: 1, age: 1 }" or "COLLSCAN"

    Returns:
        Dict with index structure and metadata
    """
    if not plan_summary or plan_summary == "N/A":
        return {"type": "COLLSCAN", "fields": {}, "structure": []}

    if plan_summary == "COLLSCAN":
        return {"type": "COLLSCAN", "fields": {}, "structure": []}

    if plan_summary == "IDHACK":
        return {"type": "IDHACK", "fields": {"_id": 1}, "structure": [("_id", 1)]}

    if plan_summary.startswith("IXSCAN"):
        try:
            index_part = plan_summary[6:].strip()

            index_part = index_part.replace("'", '"')

            index_structure = json.loads(index_part)

            structure = [(field, direction) for field, direction in index_structure.items()]

            return {"type": "IXSCAN", "fields": index_structure, "structure": structure}
        except (json.JSONDecodeError, ValueError, IndexError) as e:
            logger.warning("Failed to parse IXSCAN structure (%s): %s", plan_summary, e)
            try:
                pattern = r"(\w+):\s*([+-]?\d+)"
                matches = re.findall(pattern, index_part)
                if matches:
                    index_structure = {field: int(value) for field, value in matches}
                    structure = [(field, direction) for field, direction in index_structure.items()]
                    return {"type": "IXSCAN", "fields": index_structure, "structure": structure}
            except Exception as e2:
                logger.warning("Alternative IXSCAN parsing also failed: %s", e2)

            return {"type": "IXSCAN", "fields": {}, "structure": []}

    return {"type": "OTHER", "fields": {}, "structure": []}


def _analyze_index_coverage(
    query_fields: List[Tuple[str, str]], current_index: Dict, stats: Dict
) -> Dict:
    """Analyze how well the current index covers the query.

    Args:
        query_fields: List of (field_name, usage_type) from query pattern
        current_index: Parsed index structure from planSummary
        stats: Query execution statistics

    Returns:
        Coverage analysis with score and issues
    """
    coverage_analysis = {
        "coverage_score": 0,
        "esr_violations": [],
        "missing_fields": [],
        "suboptimal_order": [],
        "recommendation_type": "CREATE_NEW",
        "improvement_details": [],
    }

    if current_index["type"] == "COLLSCAN":
        coverage_analysis["coverage_score"] = 0
        coverage_analysis["recommendation_type"] = "CREATE_NEW"
        coverage_analysis["improvement_details"].append("Query performs full collection scan")
        return coverage_analysis

    query_field_names = [f for f, _ in query_fields]
    query_field_types = {f: t for f, t in query_fields}
    index_fields = current_index.get("fields", {})
    index_structure = current_index.get("structure", [])

    coverage_score = _calculate_coverage_score(
        query_field_names, query_field_types, index_structure, stats
    )
    coverage_analysis["coverage_score"] = coverage_score

    esr_violations = _validate_esr_compliance(index_structure, query_field_types)
    coverage_analysis["esr_violations"] = esr_violations

    missing_fields = [f for f in query_field_names if f not in index_fields]
    coverage_analysis["missing_fields"] = missing_fields

    if coverage_score == 0:
        coverage_analysis["recommendation_type"] = "CREATE_NEW"
    elif coverage_score < 50:
        coverage_analysis["recommendation_type"] = "REPLACE_INDEX"
    elif missing_fields and esr_violations:
        coverage_analysis["recommendation_type"] = "REPLACE_INDEX"
    elif missing_fields:
        coverage_analysis["recommendation_type"] = "EXTEND_INDEX"
    elif esr_violations:
        coverage_analysis["recommendation_type"] = "IMPROVE_EXISTING"
    else:
        coverage_analysis["recommendation_type"] = "OPTIMIZED"

    if esr_violations:
        coverage_analysis["improvement_details"].extend(esr_violations)
    if missing_fields:
        coverage_analysis["improvement_details"].append(
            f"Missing fields: {', '.join(missing_fields)}"
        )

    return coverage_analysis


def _calculate_coverage_score(
    query_field_names: List[str],
    query_field_types: Dict[str, str],
    index_structure: List[Tuple[str, int]],
    stats: Dict = None,
) -> int:
    """Calculate coverage score (0-100) for how well index matches query."""
    if not index_structure:
        return 0

    index_fields = [field for field, _ in index_structure]

    field_coverage = sum(1 for field in query_field_names if field in index_fields)
    total_query_fields = len(query_field_names)

    if total_query_fields == 0:
        return 100

    base_coverage = (field_coverage / total_query_fields) * 100

    esr_score = _calculate_esr_score(query_field_types, index_structure)

    final_score = (base_coverage * 0.7) + (esr_score * 0.3)

    if stats:
        mean_ms = stats.get("mean", 0)
        count = stats.get("count", 0)

        if mean_ms > 1000 and base_coverage >= 80:
            equality_fields = [f for f, t in query_field_types.items() if t == "equality"]
            if len(equality_fields) == 1 and len(query_field_names) == 1:
                final_score *= 0.8
                logger.warning(
                    "Simple query with good index but slow (%.0fms), "
                    "suggesting selectivity/data issues",
                    mean_ms,
                )

        if count > 50 and mean_ms > 500:
            logger.info("Frequent slow query count=%s mean_ms=%.0f", count, mean_ms)

    return min(int(final_score), 100)


def _calculate_esr_score(
    query_field_types: Dict[str, str], index_structure: List[Tuple[str, int]]
) -> int:
    """Calculate ESR compliance score (0-100)."""
    if not index_structure:
        return 0

    equality_fields = [f for f, t in query_field_types.items() if t == "equality"]
    sort_fields = [f for f, t in query_field_types.items() if t == "sort"]
    range_fields = [f for f, t in query_field_types.items() if t == "range"]

    index_fields = [field for field, _ in index_structure]
    esr_violations = 0

    equality_positions = [i for i, field in enumerate(index_fields) if field in equality_fields]
    sort_positions = [i for i, field in enumerate(index_fields) if field in sort_fields]
    range_positions = [i for i, field in enumerate(index_fields) if field in range_fields]

    if equality_positions and sort_positions:
        if max(equality_positions) > min(sort_positions):
            esr_violations += 1

    if equality_positions and range_positions:
        if max(equality_positions) > min(range_positions):
            esr_violations += 1

    if sort_positions and range_positions:
        if max(sort_positions) > min(range_positions):
            esr_violations += 1

    max_violations = 3
    esr_score = max(0, 100 - (esr_violations / max_violations) * 100)

    return int(esr_score)


def _validate_esr_compliance(
    index_structure: List[Tuple[str, int]], query_field_types: Dict[str, str]
) -> List[str]:
    """Validate ESR compliance and return list of violations."""
    violations = []

    if not index_structure:
        return violations

    equality_fields = [f for f, t in query_field_types.items() if t == "equality"]
    sort_fields = [f for f, t in query_field_types.items() if t == "sort"]
    range_fields = [f for f, t in query_field_types.items() if t == "range"]

    index_fields = [field for field, _ in index_structure]

    equality_positions = [i for i, field in enumerate(index_fields) if field in equality_fields]
    sort_positions = [i for i, field in enumerate(index_fields) if field in sort_fields]
    range_positions = [i for i, field in enumerate(index_fields) if field in range_fields]

    for range_pos in range_positions:
        for eq_pos in equality_positions:
            if range_pos < eq_pos:
                violations.append(
                    f"Range field '{index_fields[range_pos]}' appears before "
                    f"equality field '{index_fields[eq_pos]}'"
                )

    for range_pos in range_positions:
        for sort_pos in sort_positions:
            if range_pos < sort_pos:
                violations.append(
                    f"Range field '{index_fields[range_pos]}' appears before "
                    f"sort field '{index_fields[sort_pos]}'"
                )

    return violations


def _analyze_selectivity(
    query_field_types: Dict[str, str], stats: Dict, index_structure: List[Tuple[str, int]]
) -> Dict:
    """Analyze index selectivity based on query patterns and performance."""
    analysis = {"selectivity_score": 100, "issues": [], "recommendations": []}

    avg_docs = stats.get("avg_docs_examined")
    avg_nr = stats.get("avg_n_returned")
    avg_keys = stats.get("avg_keys_examined")
    if avg_docs is not None and avg_nr is not None and avg_nr > 0:
        ratio = avg_docs / avg_nr
        if ratio > 50:
            analysis["issues"].append(
                f"Avg docs examined per returned document is high ({ratio:.0f}:1)",
            )
    if avg_keys is not None and avg_docs is not None and avg_docs > 0:
        kr = avg_keys / avg_docs
        if kr < 0.2:
            analysis["issues"].append(
                f"Low avg keysExamined vs docsExamined ratio ({kr:.2f})",
            )

    mean_ms = stats.get("mean", 0)
    stats.get("count", 0)

    equality_fields = [f for f, t in query_field_types.items() if t == "equality"]
    range_fields = [f for f, t in query_field_types.items() if t == "range"]

    if len(equality_fields) == 1 and len(query_field_types) == 1:
        if mean_ms > 500:
            analysis["selectivity_score"] = 50
            analysis["issues"].append(
                "Simple equality query is slow - suggests poor field selectivity"
            )
            analysis["recommendations"].append(
                "Consider adding more selective filters or using compound indexes"
            )

    if range_fields:
        analysis["selectivity_score"] = min(analysis["selectivity_score"], 70)
        analysis["issues"].append(f"Range queries on {', '.join(range_fields)} are less selective")
        analysis["recommendations"].append("Consider adding equality filters before range filters")

    if len(equality_fields) > 1:
        if mean_ms > 1000:
            analysis["selectivity_score"] = 60
            analysis["issues"].append(
                "Multiple equality filters but still slow - check data distribution"
            )
            analysis["recommendations"].append("Verify field cardinality and data distribution")

    if index_structure:
        [field for field, _ in index_structure]
        for i, (field, _) in enumerate(index_structure):
            if field in equality_fields and i > 0:
                analysis["selectivity_score"] = min(analysis["selectivity_score"], 80)
                analysis["issues"].append(f"Equality field '{field}' not at start of index")
                analysis["recommendations"].append("Reorder index to put equality fields first")

    return analysis
