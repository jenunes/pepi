"""
Lightweight AI-powered index recommendations for MongoDB queries.
Uses embedded local LLM for seamless experience.
"""

import re
import json
import os
from typing import Optional, Dict, List, Tuple
from pathlib import Path

# MongoDB Official Documentation - ESR (Equality, Sort, Range) Guideline
# Source: https://www.mongodb.com/docs/manual/tutorial/equality-sort-range-guideline/
MONGODB_ESR_GUIDELINES = """
ESR (Equality, Sort, Range) Guideline for MongoDB Compound Indexes:

An index that references multiple fields is a compound index. Index keys correspond to document fields.

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
- Compound index {a: 1, b: 1} supports: sort({a: 1}), sort({a: 1, b: 1}), sort({b: 1}) with equality on 'a'
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


class IndexAdvisor:
    """Lightweight index advisor with embedded local LLM."""
    
    def __init__(self):
        self.llm = None
        self.model_path = None
        self._init_llm()
    
    def _init_llm(self):
        """Initialize embedded LLM if available."""
        try:
            from llama_cpp import Llama
            
            # Look for model in package directory
            package_dir = Path(__file__).parent
            model_dir = package_dir / "models"
            
            print(f"🔍 Looking for LLM models in: {model_dir}")
            
            # Try to find a model file
            model_files = list(model_dir.glob("*.gguf")) if model_dir.exists() else []
            
            if model_files:
                self.model_path = model_files[0]
                print(f"🤖 Loading LLM model: {self.model_path.name}")
                # Initialize with larger context for detailed explanations
                self.llm = Llama(
                    model_path=str(self.model_path),
                    n_ctx=2048,         # Larger context window for MongoDB docs + analysis
                    n_threads=4,        # More threads for better performance
                    n_gpu_layers=0,     # CPU only for portability
                    verbose=False
                )
                print(f"✅ LLM initialized successfully!")
            else:
                # LLM not available, will use pure rule-based
                print(f"⚠️  No LLM model found in {model_dir}")
                print("   Install with: pip install llama-cpp-python")
                print("   Then download model: python scripts/download_model.py")
                self.llm = None
        
        except ImportError:
            # llama-cpp-python not installed, use rule-based only
            print("⚠️  llama-cpp-python not installed (rule-based recommendations only)")
            print("   Install with: pip install llama-cpp-python")
            self.llm = None
        except Exception as e:
            print(f"❌ Failed to initialize LLM: {e}")
            self.llm = None
    
    def analyze_queries(self, query_stats: Dict, use_llm: bool = False) -> List[Dict]:
        """Analyze queries and generate index recommendations.
        
        Args:
            query_stats: Dictionary of query statistics from calculate_query_stats
            use_llm: Whether to use LLM enhancement (default: False for bulk analysis)
            
        Returns:
            List of recommendations sorted by priority
        """
        recommendations = []
        
        for (namespace, operation, pattern), stats in query_stats.items():
            # Skip system collections
            if self._is_system_collection(namespace):
                continue
            
            # Skip if already has good index
            if not self._needs_index(stats):
                continue
            
            # Calculate priority score
            priority = self._calculate_priority(stats)
            
            # Generate recommendation (without LLM for bulk analysis)
            rec = self._generate_recommendation(namespace, operation, pattern, stats, use_llm=use_llm)
            
            if rec:
                rec['priority'] = priority
                rec['priority_level'] = self._get_priority_level(priority)
                recommendations.append(rec)
        
        # Sort by priority
        recommendations.sort(key=lambda x: x['priority'], reverse=True)
        
        return recommendations
    
    def analyze_single_query(self, namespace: str, operation: str, pattern: str, stats: Dict) -> Optional[Dict]:
        """Analyze a single query with LLM enhancement (for UI button clicks).
        
        When user explicitly clicks "Get AI Recommendation", always analyze the query
        even if it has an index - the index might be suboptimal or incorrect.
        
        Args:
            namespace: Query namespace (db.collection)
            operation: Query operation (find, aggregate, etc.)
            pattern: Query pattern
            stats: Query statistics
            
        Returns:
            Single recommendation dict or None
        """
        print(f"🔎 analyze_single_query: {namespace}.{operation}")
        print(f"   Stats received: {stats}")
        print(f"   Indexes: {stats.get('indexes', 'NOT FOUND')}")
        
        # Skip system collections
        if self._is_system_collection(namespace):
            print(f"   ⚠️  Skipping system collection")
            return None
        
        # For explicit user requests, ALWAYS analyze (even with existing indexes)
        # Existing indexes might be wrong or suboptimal
        print(f"   ✅ Analyzing query (user-requested)")
        
        # Calculate priority
        priority = self._calculate_priority(stats)
        print(f"   Priority score: {priority}")
        
        # Generate recommendation WITH LLM (use_llm=True for single query)
        rec = self._generate_recommendation(namespace, operation, pattern, stats, use_llm=True)
        print(f"   Generated recommendation: {rec is not None}")
        if rec:
            print(f"   Recommendation keys: {rec.keys()}")
            rec['priority'] = priority
            rec['priority_level'] = self._get_priority_level(priority)
        
        return rec
    
    def _is_system_collection(self, namespace: str) -> bool:
        """Check if namespace is a system collection that shouldn't be touched."""
        if not namespace or '.' not in namespace:
            return False
        
        db, collection = namespace.split('.', 1)
        
        # System databases
        system_dbs = ['admin', 'config', 'local']
        if db in system_dbs:
            return True
        
        # System collections (start with system.)
        if collection.startswith('system.'):
            return True
        
        return False
    
    def _needs_index(self, stats: Dict) -> bool:
        """Check if query needs index improvement."""
        indexes = stats.get('indexes', set())
        
        # Convert to set if it's a list
        if isinstance(indexes, list):
            indexes = set(indexes)
        
        # Has COLLSCAN - always needs optimization
        if 'COLLSCAN' in indexes:
            return True
        
        # Slow query without obvious index issue
        if stats.get('mean', 0) > 100 and stats.get('count', 0) > 5:
            return True
        
        # High execution count with moderate slowness
        if stats.get('count', 0) > 50 and stats.get('mean', 0) > 50:
            return True
        
        return False
    
    def _calculate_priority(self, stats: Dict) -> float:
        """Calculate priority score (0-1000) based on impact."""
        count = stats.get('count', 0)
        mean_ms = stats.get('mean', 0)
        indexes = stats.get('indexes', set())
        
        # Convert to set if it's a list
        if isinstance(indexes, list):
            indexes = set(indexes)
        
        has_collscan = 'COLLSCAN' in indexes
        
        # Base score: total time wasted
        base_score = count * mean_ms / 1000  # Convert to seconds
        
        # Ensure minimum score for COLLSCAN
        if has_collscan and base_score < 10:
            base_score = 10  # Minimum priority for COLLSCAN
        
        # Multipliers
        if has_collscan:
            base_score *= 2  # COLLSCAN is critical
        
        if mean_ms > 200:
            base_score *= 1.5  # Very slow queries
        
        if count > 100:
            base_score *= 1.3  # High frequency
        
        return min(base_score, 1000)  # Cap at 1000
    
    def _get_priority_level(self, score: float) -> str:
        """Convert priority score to level."""
        if score >= 100:
            return "CRITICAL"
        elif score >= 50:
            return "HIGH"
        elif score >= 20:
            return "MEDIUM"
        else:
            return "LOW"
    
    def _generate_recommendation(self, namespace: str, operation: str, 
                                 pattern: str, stats: Dict, use_llm: bool = False) -> Optional[Dict]:
        """Generate index recommendation for a query."""
        
        # Parse query pattern to extract fields
        fields = self._extract_query_fields(pattern, operation)
        
        # Check if this is a COLLSCAN query
        indexes = stats.get('indexes', set())
        if isinstance(indexes, list):
            indexes = set(indexes)
        has_collscan = 'COLLSCAN' in indexes
        
        # If no fields detected but has COLLSCAN, provide generic advice
        if not fields and has_collscan:
            return self._generate_generic_collscan_recommendation(namespace, operation, pattern, stats, use_llm)
        
        if not fields:
            return None
        
        # Generate index command
        index_spec = self._build_index_spec(fields, operation, pattern)
        
        if not index_spec:
            # Has fields but couldn't build index (complex query)
            if has_collscan:
                return self._generate_generic_collscan_recommendation(namespace, operation, pattern, stats, use_llm)
            return None
        
        # For initial fast response, use rule-based explanation
        # User can request detailed LLM analysis later
        reason = self._generate_reason(fields, operation, stats)
        
        # Build recommendation
        rec = {
            'namespace': namespace,
            'operation': operation,
            'pattern': pattern[:200],  # Truncate for display
            'current_index': ', '.join(stats.get('indexes', [])) or 'COLLSCAN',
            'stats': {
                'count': stats.get('count', 0),
                'mean_ms': round(stats.get('mean', 0), 1),
                'p95_ms': round(stats.get('percentile_95', 0), 1),
            },
            'recommendation': {
                'index_spec': index_spec,
                'command': self._format_create_index(namespace, index_spec),
                'reason': reason,
            }
        }
        
        return rec
    
    def _generate_generic_collscan_recommendation(self, namespace: str, operation: str,
                                                  pattern: str, stats: Dict, use_llm: bool = False) -> Dict:
        """Generate generic recommendation for COLLSCAN queries where fields couldn't be extracted."""
        
        count = stats.get('count', 0)
        mean_ms = stats.get('mean', 0)
        
        # Generic advice for complex queries
        reason = f"Query performs full collection scan (COLLSCAN). "
        reason += f"Executed {count}× averaging {mean_ms:.0f}ms. "
        
        if operation == 'aggregate':
            reason += "This aggregation pipeline requires analysis. Review the pipeline stages ($match, $sort, $group) "
            reason += "and create indexes on fields used in $match and $sort stages."
            command = f"// Analyze pipeline and create index on filtered/sorted fields\n// db.{namespace.split('.')[-1]}.createIndex({{ field: 1 }})"
        else:
            reason += "Query pattern is complex. Analyze which fields are frequently queried and create appropriate indexes."
            command = f"// Analyze query and create index on frequently used fields\n// db.{namespace.split('.')[-1]}.createIndex({{ field: 1 }})"
        
        # Build recommendation
        rec = {
            'namespace': namespace,
            'operation': operation,
            'pattern': pattern[:200],
            'current_index': 'COLLSCAN',
            'stats': {
                'count': count,
                'mean_ms': round(mean_ms, 1),
                'p95_ms': round(stats.get('percentile_95', 0), 1),
            },
            'recommendation': {
                'index_spec': "Manual analysis required",
                'command': command,
                'reason': reason,
                'estimated_improvement': self._estimate_improvement(stats),
            }
        }
        
        # Add LLM insight if available
        if use_llm and self.llm:
            llm_insight = self._get_llm_insight_for_complex_query(namespace, operation, pattern, stats)
            if llm_insight:
                rec['recommendation']['additional_tip'] = llm_insight
        
        return rec
    
    def _should_use_llm(self, stats: Dict) -> bool:
        """Decide if LLM enhancement is worth it for this query."""
        # Only use LLM for high-priority queries to keep it fast
        priority = self._calculate_priority(stats)
        return priority >= 50  # HIGH or CRITICAL only
    
    def _extract_query_fields(self, pattern: str, operation: str) -> List[Tuple[str, str]]:
        """Extract fields and their usage from query pattern.
        
        Returns:
            List of (field_name, usage_type) tuples
            usage_type: 'equality', 'range', 'sort', 'text'
        """
        fields = []
        
        print(f"🔍 Extracting fields from {operation} pattern (first 200 chars): {pattern[:200]}")
        
        try:
            # Handle different operation types
            if operation == 'find':
                # Parse JSON-like pattern
                # Look for field patterns like {"field": value} or {"field": {"$gt": value}}
                
                # Equality matches
                equality_fields = re.findall(r'"([^"]+)":\s*"[^"]*"', pattern)
                for field in equality_fields:
                    if not field.startswith('$'):
                        fields.append((field, 'equality'))
                
                # Range operators
                range_patterns = [r'"([^"]+)":\s*\{"(\$gt|\$gte|\$lt|\$lte|\$ne)"', 
                                 r'"([^"]+)":\s*\{"(\$in|\$nin)"']
                for pat in range_patterns:
                    matches = re.findall(pat, pattern)
                    for field, op in matches:
                        if not field.startswith('$'):
                            fields.append((field, 'range'))
                
                # Text search
                if '$text' in pattern or '$regex' in pattern:
                    text_fields = re.findall(r'"([^"]+)":\s*\{"\$regex"', pattern)
                    for field in text_fields:
                        fields.append((field, 'text'))
                
                # Sort detection
                if '"sort"' in pattern.lower():
                    sort_fields = re.findall(r'"sort":\s*\{[^}]*"([^"]+)"', pattern)
                    for field in sort_fields:
                        if not field.startswith('$'):
                            fields.append((field, 'sort'))
            
            elif operation == 'aggregate':
                # Parse aggregation pipeline - try JSON parsing first for structured pipelines
                try:
                    # Try to parse as JSON array (full pipeline)
                    pipeline = json.loads(pattern)
                    if isinstance(pipeline, list):
                        for stage in pipeline:
                            if isinstance(stage, dict):
                                # Extract $match fields
                                if '$match' in stage:
                                    match_clause = stage['$match']
                                    for field_name, field_value in match_clause.items():
                                        if not field_name.startswith('$'):
                                            # Check if it's a range query
                                            if isinstance(field_value, dict) and any(op.startswith('$') for op in field_value.keys()):
                                                fields.append((field_name, 'range'))
                                            else:
                                                fields.append((field_name, 'equality'))
                                
                                # Extract $sort fields
                                if '$sort' in stage:
                                    sort_clause = stage['$sort']
                                    for field_name in sort_clause.keys():
                                        if not field_name.startswith('$'):
                                            fields.append((field_name, 'sort'))
                        
                        print(f"✅ JSON parsing succeeded! Extracted {len(fields)} fields: {fields}")
                except (json.JSONDecodeError, TypeError) as e:
                    print(f"⚠️ JSON parsing failed: {e}")
                    # Fallback to regex-based extraction for simplified patterns
                    
                    # Extract $match fields (multiple $match stages possible)
                    match_sections = re.findall(r'\$match[,\s]*\{[^}]*\}', pattern)
                    for match_section in match_sections:
                        match_fields = re.findall(r'"([^"$][^"]*)":\s*["{[]', match_section)
                        for field in match_fields:
                            if not field.startswith('$') and field not in ('$match', '$sort', '$group'):
                                fields.append((field, 'equality'))
                    
                    # Extract $sort fields
                    sort_sections = re.findall(r'\$sort[,\s]*\{[^}]*\}', pattern)
                    for sort_section in sort_sections:
                        sort_fields = re.findall(r'"([^"$][^"]*)":\s*[-\d]', sort_section)
                        for field in sort_fields:
                            if not field.startswith('$'):
                                fields.append((field, 'sort'))
                    
                    # Also try simple pattern extraction as fallback
                    if not fields:
                        all_fields = re.findall(r'"([a-zA-Z_][a-zA-Z0-9_\.]*)":', pattern)
                        for field in all_fields:
                            if not field.startswith('$') and field not in ('$match', '$sort', '$group', '$project', '$lookup', '$unwind'):
                                if (field, 'equality') not in fields and (field, 'sort') not in fields:
                                    fields.append((field, 'equality'))
            
            elif operation in ('update', 'delete'):
                # Extract query fields
                query_fields = re.findall(r'"q":\s*\{[^}]*"([^"]+)":', pattern)
                for field in query_fields:
                    if not field.startswith('$'):
                        fields.append((field, 'equality'))
        
        except Exception:
            pass
        
        # Remove duplicates while preserving order
        seen = set()
        unique_fields = []
        for field, usage in fields:
            key = (field, usage)
            if key not in seen:
                seen.add(key)
                unique_fields.append(key)
        
        print(f"🎯 Final unique fields: {unique_fields}")
        
        return unique_fields
    
    def _build_index_spec(self, fields: List[Tuple[str, str]], 
                          operation: str, pattern: str) -> Optional[Dict]:
        """Build index specification from extracted fields."""
        
        if not fields:
            return None
        
        index_spec = {}
        
        # ESR Rule: Equality, Sort, Range
        equality_fields = [f for f, u in fields if u == 'equality']
        sort_fields = [f for f, u in fields if u == 'sort']
        range_fields = [f for f, u in fields if u == 'range']
        text_fields = [f for f, u in fields if u == 'text']
        
        # Text index
        if text_fields:
            for field in text_fields:
                index_spec[field] = 'text'
            return index_spec
        
        # Apply ESR rule
        for field in equality_fields:
            index_spec[field] = 1
        
        for field in sort_fields:
            if field not in index_spec:
                index_spec[field] = 1
        
        for field in range_fields:
            if field not in index_spec:
                index_spec[field] = 1
        
        return index_spec if index_spec else None
    
    def _format_create_index(self, namespace: str, index_spec: Dict) -> str:
        """Format MongoDB createIndex command with explicit database and collection."""
        db, collection = namespace.split('.', 1) if '.' in namespace else ('db', namespace)
        
        # Format index spec with proper indentation for readability
        spec_str = json.dumps(index_spec, indent=2)
        
        return f'db.getSiblingDB("{db}").getCollection("{collection}").createIndex({spec_str})'
    
    def _generate_reason(self, fields: List[Tuple[str, str]], 
                        operation: str, stats: Dict) -> str:
        """Generate human-readable reason for recommendation."""
        
        indexes = stats.get('indexes', set())
        # Convert to set if it's a list
        if isinstance(indexes, list):
            indexes = set(indexes)
        
        has_collscan = 'COLLSCAN' in indexes
        count = stats.get('count', 0)
        mean_ms = stats.get('mean', 0)
        
        field_names = [f for f, _ in fields]
        field_types = {f: t for f, t in fields}
        
        if has_collscan:
            reason = f"Query performs full collection scan. "
            
            # Be specific about field usage
            equality = [f for f, t in fields if t == 'equality']
            ranges = [f for f, t in fields if t == 'range']
            sorts = [f for f, t in fields if t == 'sort']
            
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
            reason = f"Query is slow ({mean_ms:.0f}ms average, {count}× executions). "
            reason += f"Current index may not be optimal for this access pattern. "
            reason += "A better-targeted compound index could significantly improve performance."
        
        return reason
    
    def _estimate_improvement(self, stats: Dict) -> str:
        """Estimate performance improvement."""
        
        indexes = stats.get('indexes', set())
        # Convert to set if it's a list
        if isinstance(indexes, list):
            indexes = set(indexes)
        
        has_collscan = 'COLLSCAN' in indexes
        mean_ms = stats.get('mean', 0)
        
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
    
    def _get_llm_analysis(self, namespace: str, operation: str, 
                          fields: List[Tuple[str, str]], index_spec: Dict, 
                          stats: Dict) -> Dict:
        """Analyze query and determine if optimization is needed, or if current index is sufficient."""
        
        if not self.llm:
            return {'is_optimized': False, 'explanation': self._generate_reason(fields, operation, stats)}
        
        try:
            # Build detailed context for LLM
            field_details = []
            for field_name, usage_type in fields:
                field_details.append(f"{field_name} ({usage_type})")
            
            index_fields = ', '.join(index_spec.keys())
            current_indexes = stats.get('indexes', ['COLLSCAN'])
            current_index_str = current_indexes[0] if current_indexes else 'COLLSCAN'
            
            prompt = f"""<|system|>You are a senior MongoDB database performance engineer. Use the official MongoDB documentation provided to give accurate, authoritative recommendations.

{MONGODB_ESR_GUIDELINES}

{MONGODB_INDEX_STRATEGIES}<|end|>
<|user|>Analyze this MongoDB query:

Collection: {namespace}
Operation: {operation}
Current Index: {current_index_str}
Execution Statistics:
- Executions: {stats.get('count', 0)}×
- Average duration: {stats.get('mean', 0):.1f}ms
- P95 latency: {stats.get('percentile_95', 0):.1f}ms

Query Field Usage:
{chr(10).join([f"  • {name} ({usage})" for name, usage in fields])}

Our Recommended Index:
{chr(10).join([f"  • {field}: 1" for field in index_spec.keys()])}

Task: Determine if the current index ({current_index_str}) is already optimal, or if our recommended index would improve performance.

Respond in this format:
STATUS: [OPTIMIZED or NEEDS_OPTIMIZATION]

EXPLANATION:
[If OPTIMIZED: Explain why the current index is already good and follows ESR principles]
[If NEEDS_OPTIMIZATION: Provide detailed analysis with these sections:
**Current Performance Issue:**
**ESR Principle Application:**
**Performance Improvements:**
**Query Plan Changes:**]

Be honest - if the query is already well-indexed and performing acceptably, say STATUS: OPTIMIZED.<|end|>
<|assistant|>STATUS: """

            response = self.llm(
                prompt,
                max_tokens=450,
                temperature=0.7,
                stop=["<|end|>", "<|user|>"],
                echo=False
            )
            
            text = response['choices'][0]['text'].strip()
            print(f"🤖 LLM analysis generated: {len(text)} chars")
            
            # Parse the response
            if text.startswith('OPTIMIZED'):
                # Extract explanation after "EXPLANATION:"
                explanation_start = text.find('EXPLANATION:')
                explanation = text[explanation_start + 12:].strip() if explanation_start != -1 else text
                print(f"✅ LLM says query is already optimized")
                return {'is_optimized': True, 'explanation': explanation or 'Current index is already optimal for this query pattern.'}
            else:
                # NEEDS_OPTIMIZATION - extract explanation
                explanation_start = text.find('EXPLANATION:')
                explanation = text[explanation_start + 12:].strip() if explanation_start != -1 else text
                print(f"⚠️ LLM recommends optimization")
                return {'is_optimized': False, 'explanation': explanation or self._generate_reason(fields, operation, stats)}
        
        except Exception as e:
            print(f"❌ LLM analysis failed: {e}")
            return {'is_optimized': False, 'explanation': self._generate_reason(fields, operation, stats)}
    
    def _get_llm_insight(self, namespace: str, operation: str, pattern: str,
                         fields: List[Tuple[str, str]], stats: Dict) -> Optional[str]:
        """Get additional insight from embedded LLM."""
        
        if not self.llm:
            return None
        
        try:
            # Build concise prompt
            field_list = ', '.join([f"{f} ({t})" for f, t in fields[:3]])  # Top 3 fields only
            
            prompt = f"""<|system|>You are a MongoDB database expert providing index optimization advice.<|end|>
<|user|>A MongoDB {operation} on {namespace} performs COLLSCAN scanning {stats.get('count', 0)} times averaging {stats.get('mean', 0):.0f}ms.
Fields used: {field_list}

Provide one specific optimization tip in 15 words or less.<|end|>
<|assistant|>"""

            # Generate with tight constraints for speed
            response = self.llm(
                prompt,
                max_tokens=50,           # Very short
                temperature=0.7,         # More creative
                stop=["<|end|>", "\n\n"],  # Stop at end of response
                echo=False
            )
            
            text = response['choices'][0]['text'].strip()
            print(f"🤖 LLM insight (raw): '{text}'")
            
            # Return only if meaningful
            if len(text) > 10 and len(text.split()) <= 25:
                print(f"✅ LLM insight accepted: '{text}'")
                return text
            else:
                print(f"⚠️ LLM insight rejected (too short/long): '{text}'")
        
        except Exception as e:
            print(f"❌ LLM insight failed: {e}")
            pass
        
        return None
    
    def _get_llm_insight_for_complex_query(self, namespace: str, operation: str, 
                                           pattern: str, stats: Dict) -> Optional[str]:
        """Get LLM insight for complex queries where fields couldn't be auto-detected."""
        
        if not self.llm:
            return None
        
        try:
            prompt = f"""MongoDB {operation} on {namespace} has COLLSCAN.
Pattern (truncated): {pattern[:150]}
Executions: {stats.get('count', 0)}× avg {stats.get('mean', 0):.0f}ms

Suggest which fields to index (one sentence, max 25 words):"""

            response = self.llm(
                prompt,
                max_tokens=60,
                temperature=0.3,
                stop=["\n"],
                echo=False
            )
            
            text = response['choices'][0]['text'].strip()
            
            if len(text) > 10 and len(text.split()) <= 30:
                return text
        
        except Exception:
            pass
        
        return None
