"""
Rule-based index recommendations for MongoDB queries.
Provides accurate, fast analysis without AI dependencies.
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
    """Rule-based index advisor with fast, accurate analysis."""
    
    def __init__(self):
        pass
    
    
    def analyze_queries(self, query_stats: Dict) -> List[Dict]:
        """Analyze queries and generate index recommendations.
        
        Args:
            query_stats: Dictionary of query statistics from calculate_query_stats
            
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
            
            # Generate recommendation
            rec = self._generate_recommendation(namespace, operation, pattern, stats)
            
            if rec:
                rec['priority'] = priority
                rec['priority_level'] = self._get_priority_level(priority)
                recommendations.append(rec)
        
        # Sort by priority
        recommendations.sort(key=lambda x: x['priority'], reverse=True)
        
        return recommendations
    
    def analyze_single_query(self, namespace: str, operation: str, pattern: str, stats: Dict) -> Optional[Dict]:
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
        print(f"🔎 analyze_single_query: {namespace}.{operation}")
        print(f"   Stats received: {stats}")
        print(f"   Indexes: {stats.get('indexes', 'NOT FOUND')}")
        
        # Skip system collections
        if self._is_system_collection(namespace):
            print(f"   ⚠️  Skipping system collection")
            return None
        
        # Parse query pattern to extract fields
        fields = self._extract_query_fields(pattern, operation)
        if not fields:
            print(f"   ⚠️  No fields detected")
            return None
        
        # Get current index information and perform coverage analysis
        current_index_info = self._get_current_index_info(stats)
        coverage_analysis = self._analyze_index_coverage(fields, current_index_info, stats)
        
        # CRITICAL FIX: Skip AI analysis for OPTIMIZED queries
        if (coverage_analysis['recommendation_type'] == 'OPTIMIZED' and 
            coverage_analysis['coverage_score'] >= 90):
            print(f"   ✅ Query is already optimally indexed (coverage: {coverage_analysis['coverage_score']}%)")
            print(f"   ⚠️  Skipping AI analysis to prevent hallucinations")
            return None
        
        print(f"   ✅ Analyzing query (user-requested)")
        
        # Calculate priority
        priority = self._calculate_priority(stats)
        print(f"   Priority score: {priority}")
        
        # Generate recommendation
        rec = self._generate_recommendation(namespace, operation, pattern, stats)
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
                                 pattern: str, stats: Dict) -> Optional[Dict]:
        """Generate index recommendation for a query."""
        
        # Parse query pattern to extract fields
        fields = self._extract_query_fields(pattern, operation)
        
        # Get current index information from planSummary
        current_index_info = self._get_current_index_info(stats)
        
        # If no fields detected but has COLLSCAN, provide generic advice
        if not fields and current_index_info['type'] == 'COLLSCAN':
            return self._generate_generic_collscan_recommendation(namespace, operation, pattern, stats)
        
        if not fields:
            return None
        
        # Perform coverage analysis
        coverage_analysis = self._analyze_index_coverage(fields, current_index_info, stats)
        
        # CRITICAL FIX: Skip recommendations for OPTIMIZED queries
        if (coverage_analysis['recommendation_type'] == 'OPTIMIZED' and 
            coverage_analysis['coverage_score'] >= 90):
            print(f"✅ Query is already optimally indexed (coverage: {coverage_analysis['coverage_score']}%)")
            return None
        
        # Generate index command
        index_spec = self._build_index_spec(fields, operation, pattern)
        
        if not index_spec:
            # Has fields but couldn't build index (complex query)
            if current_index_info['type'] == 'COLLSCAN':
                return self._generate_generic_collscan_recommendation(namespace, operation, pattern, stats)
            return None
        
        # Generate migration strategy
        migration_strategy = self._generate_migration_strategy(
            namespace, current_index_info, index_spec, coverage_analysis
        )
        
        # For initial fast response, use rule-based explanation
        # User can request detailed LLM analysis later
        reason = self._generate_reason(fields, operation, stats, coverage_analysis)
        
        # Build enhanced recommendation
        rec = {
            'namespace': namespace,
            'operation': operation,
            'pattern': pattern[:200],  # Truncate for display
            'current_index': current_index_info['type'],
            'current_index_structure': current_index_info.get('structure', []),
            'stats': {
                'count': stats.get('count', 0),
                'mean_ms': round(stats.get('mean', 0), 1),
                'p95_ms': round(stats.get('percentile_95', 0), 1),
            },
            'coverage_analysis': coverage_analysis,
            'recommendation': {
                'index_spec': index_spec,
                'command': self._format_create_index(namespace, index_spec),
                'reason': reason,
                'migration_strategy': migration_strategy,
            }
        }
        
        return rec
    
    def _get_current_index_info(self, stats: Dict) -> Dict:
        """Extract current index information from stats.
        
        Args:
            stats: Query statistics containing index information
            
        Returns:
            Parsed index structure
        """
        # Get planSummary from stats if available
        plan_summary = stats.get('plan_summary', 'COLLSCAN')
        
        # Parse the planSummary to get index structure
        return self._parse_plan_summary(plan_summary)
    
    def _generate_migration_strategy(self, namespace: str, current_index: Dict, 
                                   recommended_index: Dict, coverage_analysis: Dict) -> Dict:
        """Generate migration strategy for index changes.
        
        Args:
            namespace: Database.collection name
            current_index: Current index structure
            recommended_index: Recommended index specification
            coverage_analysis: Coverage analysis results
            
        Returns:
            Migration strategy with commands and warnings
        """
        strategy = {
            'type': coverage_analysis['recommendation_type'],
            'commands': [],
            'warnings': [],
            'estimated_impact': 'low'
        }
        
        rec_type = coverage_analysis['recommendation_type']
        
        if rec_type == 'CREATE_NEW':
            # Simple case - just create new index
            strategy['commands'].append({
                'action': 'create',
                'command': self._format_create_index(namespace, recommended_index),
                'description': 'Create new index'
            })
            strategy['estimated_impact'] = 'low'
            
        elif rec_type == 'IMPROVE_EXISTING':
            # Need to replace existing index with better field order
            strategy['warnings'].append('This will replace an existing index. Monitor performance during index build.')
            strategy['commands'].extend([
                {
                    'action': 'create',
                    'command': self._format_create_index(namespace, recommended_index),
                    'description': 'Create improved index'
                },
                {
                    'action': 'drop',
                    'command': f'// Drop old index after new one is created\n// db.{namespace.split(".")[-1]}.dropIndex({{ /* old index spec */ }})',
                    'description': 'Drop old index (after verifying new one works)'
                }
            ])
            strategy['estimated_impact'] = 'medium'
            
        elif rec_type == 'EXTEND_INDEX':
            # Add fields to existing index
            strategy['warnings'].append('Consider if extending index is better than creating a new compound index.')
            strategy['commands'].append({
                'action': 'create',
                'command': self._format_create_index(namespace, recommended_index),
                'description': 'Create extended index with additional fields'
            })
            strategy['estimated_impact'] = 'low'
            
        elif rec_type == 'REPLACE_INDEX':
            # Complete replacement needed
            strategy['warnings'].append('Complete index replacement required. Plan for maintenance window.')
            strategy['commands'].extend([
                {
                    'action': 'create',
                    'command': self._format_create_index(namespace, recommended_index),
                    'description': 'Create new optimized index'
                },
                {
                    'action': 'drop',
                    'command': f'// Drop old index after verification\n// db.{namespace.split(".")[-1]}.dropIndex({{ /* old index spec */ }})',
                    'description': 'Drop old index (after verification)'
                }
            ])
            strategy['estimated_impact'] = 'high'
            
        elif rec_type == 'OPTIMIZED':
            # Index is already optimal
            strategy['commands'].append({
                'action': 'none',
                'command': '// Index is already optimal for this query pattern',
                'description': 'No changes needed'
            })
            strategy['estimated_impact'] = 'none'
        
        return strategy
    
    def _generate_generic_collscan_recommendation(self, namespace: str, operation: str, 
                                                  pattern: str, stats: Dict) -> Dict:
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
        
        
        return rec
    
    
    def _extract_fields_from_match_clause(self, match_clause: dict, fields: List[Tuple[str, str]]):
        """Recursively extract fields from a $match clause, handling $and, $or, $nor.
        
        Args:
            match_clause: The match clause dictionary
            fields: List to append extracted fields to (modified in place)
        """
        for field_name, field_value in match_clause.items():
            # Handle logical operators that contain arrays of conditions
            if field_name in ('$and', '$or', '$nor'):
                if isinstance(field_value, list):
                    for condition in field_value:
                        if isinstance(condition, dict):
                            self._extract_fields_from_match_clause(condition, fields)
            # Skip other $ operators at top level
            elif field_name.startswith('$'):
                continue
            # Regular field
            else:
                # Determine if it's equality or range
                if isinstance(field_value, dict):
                    # Check for query operators
                    operators = [k for k in field_value.keys() if k.startswith('$')]
                    if operators:
                        # Check specific operator types
                        range_ops = {'$gt', '$gte', '$lt', '$lte', '$ne', '$nin', '$regex', '$exists'}
                        in_op = {'$in'}
                        eq_op = {'$eq'}
                        
                        if any(op in range_ops for op in operators):
                            fields.append((field_name, 'range'))
                        elif any(op in in_op for op in operators):
                            # Check $in array size if possible
                            in_value = field_value.get('$in', [])
                            if isinstance(in_value, list) and len(in_value) >= 201:
                                fields.append((field_name, 'range'))
                            else:
                                fields.append((field_name, 'equality'))
                        elif any(op in eq_op for op in operators):
                            fields.append((field_name, 'equality'))
                        else:
                            # Unknown operator, assume range
                            fields.append((field_name, 'range'))
                    else:
                        # Dict without operators (nested document equality)
                        fields.append((field_name, 'equality'))
                else:
                    # Simple value equality
                    fields.append((field_name, 'equality'))
    
    def _extract_query_fields(self, pattern: str, operation: str) -> List[Tuple[str, str]]:
        """Extract fields and their usage from query pattern.
        
        Now accepts the FULL command object (including filter, sort, projection, etc.)
        instead of just normalized patterns.
        
        Returns:
            List of (field_name, usage_type) tuples
            usage_type: 'equality', 'range', 'sort', 'text'
        """
        fields = []
        
        print(f"🔍 Extracting fields from {operation} pattern (first 200 chars): {pattern[:200]}")
        
        try:
            # Handle different operation types
            if operation == 'find':
                # Try JSON parsing first for structured queries
                try:
                    query = json.loads(pattern)
                    if isinstance(query, dict):
                        # Check if this is a full command object (with 'find' key) or just a filter
                        if 'find' in query:
                            # Full command object: extract filter, sort, etc.
                            print(f"✅ Detected full find command object")
                            
                            # Extract from filter
                            filter_clause = query.get('filter', {})
                            if filter_clause:
                                self._extract_fields_from_match_clause(filter_clause, fields)
                            
                            # Extract from sort
                            sort_clause = query.get('sort', {})
                            if sort_clause and isinstance(sort_clause, dict):
                                for field_name in sort_clause.keys():
                                    if not field_name.startswith('$'):
                                        fields.append((field_name, 'sort'))
                            
                            # Note: projection fields are NOT indexed, so we skip them
                            print(f"✅ Extracted {len(fields)} fields from full command: {fields}")
                        else:
                            # Just a filter object (backward compatibility)
                            self._extract_fields_from_match_clause(query, fields)
                            print(f"✅ JSON parsing succeeded for find! Extracted {len(fields)} fields: {fields}")
                except (json.JSONDecodeError, TypeError) as e:
                    print(f"⚠️ JSON parsing failed for find: {e}, falling back to regex")
                    # Fallback to regex-based extraction
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
                    parsed = json.loads(pattern)
                    
                    # Check if this is a full command object (with 'aggregate' key) or just a pipeline
                    if isinstance(parsed, dict) and 'aggregate' in parsed:
                        # Full command object: extract pipeline
                        print(f"✅ Detected full aggregate command object")
                        pipeline = parsed.get('pipeline', [])
                    elif isinstance(parsed, list):
                        # Just a pipeline array (backward compatibility)
                        pipeline = parsed
                    else:
                        # Unknown format
                        print(f"⚠️ Unknown aggregate format: {type(parsed)}")
                        pipeline = []
                    
                    if isinstance(pipeline, list):
                        for stage in pipeline:
                            if isinstance(stage, dict):
                                # Extract $match fields (recursively handles $and, $or, $nor)
                                if '$match' in stage:
                                    match_clause = stage['$match']
                                    self._extract_fields_from_match_clause(match_clause, fields)
                                
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
                # Try JSON parsing first for full command objects
                try:
                    parsed = json.loads(pattern)
                    if isinstance(parsed, dict):
                        # Check if this is a full command object (with 'update'/'delete' key)
                        if operation in parsed:
                            print(f"✅ Detected full {operation} command object")
                            # Extract from updates/deletes array
                            items = parsed.get('updates', []) if operation == 'update' else parsed.get('deletes', [])
                            for item in items:
                                if isinstance(item, dict) and 'q' in item:
                                    query_clause = item['q']
                                    if isinstance(query_clause, dict):
                                        self._extract_fields_from_match_clause(query_clause, fields)
                        else:
                            # Just extract query fields using regex fallback
                            query_fields = re.findall(r'"q":\s*\{[^}]*"([^"]+)":', pattern)
                            for field in query_fields:
                                if not field.startswith('$'):
                                    fields.append((field, 'equality'))
                except (json.JSONDecodeError, TypeError):
                    # Fallback to regex-based extraction
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
                        operation: str, stats: Dict, coverage_analysis: Dict = None) -> str:
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
        
        # Check if this is an OPTIMIZED query (should not reach here due to skip logic, but safety check)
        if coverage_analysis and coverage_analysis.get('recommendation_type') == 'OPTIMIZED':
            reason = f"Index structure is optimal and follows ESR principles. "
            reason += f"Query slowness ({mean_ms:.0f}ms average, {count}× executions) is likely due to: "
            reason += "data volume, poor selectivity, or result set size. "
            reason += "Consider adding more selective filters, using limit, or investigating data patterns."
            return reason
        
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
            # Query has an index but is still slow - check coverage analysis
            if coverage_analysis:
                coverage_score = coverage_analysis.get('coverage_score', 0)
                esr_violations = coverage_analysis.get('esr_violations', [])
                missing_fields = coverage_analysis.get('missing_fields', [])
                
                if coverage_score < 50:
                    reason = f"Current index has poor coverage ({coverage_score}%) for this query pattern. "
                    if missing_fields:
                        reason += f"Missing fields: {', '.join(missing_fields)}. "
                    if esr_violations:
                        reason += f"ESR violations: {'; '.join(esr_violations[:2])}. "
                elif esr_violations:
                    reason = f"Index exists but violates ESR principles: {'; '.join(esr_violations[:2])}. "
                    reason += f"Query executed {count}× averaging {mean_ms:.0f}ms. "
                    reason += "Reordering index fields will improve performance."
                else:
                    reason = f"Query is slow ({mean_ms:.0f}ms average, {count}× executions) despite having an index. "
                    reason += "Current index may not be optimal for this access pattern. "
                    reason += "A better-targeted compound index could significantly improve performance."
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
    
    
    
    
    def _parse_plan_summary(self, plan_summary: str) -> Dict:
        """Parse planSummary to extract index structure and type.
        
        Args:
            plan_summary: String like "IXSCAN { status: 1, age: 1 }" or "COLLSCAN"
            
        Returns:
            Dict with index structure and metadata
        """
        if not plan_summary or plan_summary == 'N/A':
            return {'type': 'COLLSCAN', 'fields': {}, 'structure': []}
        
        # Handle COLLSCAN
        if plan_summary == 'COLLSCAN':
            return {'type': 'COLLSCAN', 'fields': {}, 'structure': []}
        
        # Handle IDHACK (uses _id index)
        if plan_summary == 'IDHACK':
            return {'type': 'IDHACK', 'fields': {'_id': 1}, 'structure': [('_id', 1)]}
        
        # Handle IXSCAN with index structure
        if plan_summary.startswith('IXSCAN'):
            # Extract the index structure part
            # Format: "IXSCAN { field1: 1, field2: -1 }"
            try:
                # Find the part after "IXSCAN "
                index_part = plan_summary[6:].strip()  # Remove "IXSCAN "
                
                # Parse the JSON-like structure
                # Replace single quotes with double quotes for JSON parsing
                index_part = index_part.replace("'", '"')
                
                # Parse as JSON
                index_structure = json.loads(index_part)
                
                # Convert to list of tuples for easier processing
                structure = [(field, direction) for field, direction in index_structure.items()]
                
                return {
                    'type': 'IXSCAN',
                    'fields': index_structure,
                    'structure': structure
                }
            except (json.JSONDecodeError, ValueError, IndexError) as e:
                print(f"⚠️ Failed to parse IXSCAN structure: {plan_summary} - {e}")
                # Try alternative parsing for MongoDB format
                try:
                    # Handle format like "IXSCAN { status: 1, age: 1 }"
                    import re
                    # Extract field: value pairs
                    pattern = r'(\w+):\s*([+-]?\d+)'
                    matches = re.findall(pattern, index_part)
                    if matches:
                        index_structure = {field: int(value) for field, value in matches}
                        structure = [(field, direction) for field, direction in index_structure.items()]
                        return {
                            'type': 'IXSCAN',
                            'fields': index_structure,
                            'structure': structure
                        }
                except Exception as e2:
                    print(f"⚠️ Alternative parsing also failed: {e2}")
                
                return {'type': 'IXSCAN', 'fields': {}, 'structure': []}
        
        # Handle other cases (TEXT, GEO, etc.)
        return {'type': 'OTHER', 'fields': {}, 'structure': []}
    
    def _analyze_index_coverage(self, query_fields: List[Tuple[str, str]], 
                                current_index: Dict, stats: Dict) -> Dict:
        """Analyze how well the current index covers the query.
        
        Args:
            query_fields: List of (field_name, usage_type) from query pattern
            current_index: Parsed index structure from planSummary
            stats: Query execution statistics
            
        Returns:
            Coverage analysis with score and issues
        """
        coverage_analysis = {
            'coverage_score': 0,
            'esr_violations': [],
            'missing_fields': [],
            'suboptimal_order': [],
            'recommendation_type': 'CREATE_NEW',
            'improvement_details': []
        }
        
        # If COLLSCAN, no coverage
        if current_index['type'] == 'COLLSCAN':
            coverage_analysis['coverage_score'] = 0
            coverage_analysis['recommendation_type'] = 'CREATE_NEW'
            coverage_analysis['improvement_details'].append("Query performs full collection scan")
            return coverage_analysis
        
        # Extract field information
        query_field_names = [f for f, _ in query_fields]
        query_field_types = {f: t for f, t in query_fields}
        index_fields = current_index.get('fields', {})
        index_structure = current_index.get('structure', [])
        
        # Calculate coverage score with performance context
        coverage_score = self._calculate_coverage_score(query_field_names, query_field_types, index_structure, stats)
        coverage_analysis['coverage_score'] = coverage_score
        
        # Check for ESR violations
        esr_violations = self._validate_esr_compliance(index_structure, query_field_types)
        coverage_analysis['esr_violations'] = esr_violations
        
        # Find missing fields
        missing_fields = [f for f in query_field_names if f not in index_fields]
        coverage_analysis['missing_fields'] = missing_fields
        
        # Determine recommendation type
        if coverage_score == 0:
            coverage_analysis['recommendation_type'] = 'CREATE_NEW'
        elif coverage_score < 50:
            coverage_analysis['recommendation_type'] = 'REPLACE_INDEX'
        elif missing_fields and esr_violations:
            coverage_analysis['recommendation_type'] = 'REPLACE_INDEX'
        elif missing_fields:
            coverage_analysis['recommendation_type'] = 'EXTEND_INDEX'
        elif esr_violations:
            coverage_analysis['recommendation_type'] = 'IMPROVE_EXISTING'
        else:
            coverage_analysis['recommendation_type'] = 'OPTIMIZED'
        
        # Generate improvement details
        if esr_violations:
            coverage_analysis['improvement_details'].extend(esr_violations)
        if missing_fields:
            coverage_analysis['improvement_details'].append(f"Missing fields: {', '.join(missing_fields)}")
        
        return coverage_analysis
    
    def _calculate_coverage_score(self, query_field_names: List[str], 
                                 query_field_types: Dict[str, str], 
                                 index_structure: List[Tuple[str, int]], 
                                 stats: Dict = None) -> int:
        """Calculate coverage score (0-100) for how well index matches query.
        
        Args:
            query_field_names: Fields used in query
            query_field_types: Field usage types (equality, range, sort)
            index_structure: Current index structure as list of (field, direction)
            stats: Query statistics for performance context
            
        Returns:
            Coverage score 0-100
        """
        if not index_structure:
            return 0
        
        index_fields = [field for field, _ in index_structure]
        
        # Check if all query fields are in index
        field_coverage = sum(1 for field in query_field_names if field in index_fields)
        total_query_fields = len(query_field_names)
        
        if total_query_fields == 0:
            return 100  # No fields to cover
        
        # Base coverage percentage
        base_coverage = (field_coverage / total_query_fields) * 100
        
        # Check ESR compliance
        esr_score = self._calculate_esr_score(query_field_types, index_structure)
        
        # Combine coverage and ESR compliance
        final_score = (base_coverage * 0.7) + (esr_score * 0.3)
        
        # Performance context adjustment
        if stats:
            mean_ms = stats.get('mean', 0)
            count = stats.get('count', 0)
            
            # If query is very slow with good field coverage, investigate further
            if mean_ms > 1000 and base_coverage >= 80:
                # Check if it's a simple equality query (should be fast)
                equality_fields = [f for f, t in query_field_types.items() if t == 'equality']
                if len(equality_fields) == 1 and len(query_field_names) == 1:
                    # Simple equality query with good index but still slow
                    # This suggests poor selectivity or data volume issues
                    # Reduce score to indicate the index might not be helping much
                    final_score *= 0.8
                    print(f"   ⚠️  Performance context: Simple query with good index but slow ({mean_ms:.0f}ms)")
                    print(f"   ⚠️  This suggests poor selectivity or data volume issues")
            
            # If query is executed frequently and is slow, it needs attention
            if count > 50 and mean_ms > 500:
                # Don't reduce score for frequent slow queries - they need optimization
                # But add context for better recommendations
                print(f"   📊 Performance context: Frequent ({count}×) slow query ({mean_ms:.0f}ms)")
        
        return min(int(final_score), 100)
    
    def _calculate_esr_score(self, query_field_types: Dict[str, str], 
                             index_structure: List[Tuple[str, int]]) -> int:
        """Calculate ESR compliance score (0-100).
        
        Args:
            query_field_types: Field usage types from query
            index_structure: Index structure as list of (field, direction)
            
        Returns:
            ESR score 0-100
        """
        if not index_structure:
            return 0
        
        # Group fields by type
        equality_fields = [f for f, t in query_field_types.items() if t == 'equality']
        sort_fields = [f for f, t in query_field_types.items() if t == 'sort']
        range_fields = [f for f, t in query_field_types.items() if t == 'range']
        
        # Check ESR order in index
        index_fields = [field for field, _ in index_structure]
        esr_violations = 0
        
        # Find positions of each type in index
        equality_positions = [i for i, field in enumerate(index_fields) if field in equality_fields]
        sort_positions = [i for i, field in enumerate(index_fields) if field in sort_fields]
        range_positions = [i for i, field in enumerate(index_fields) if field in range_fields]
        
        # ESR Rule: Equality fields should come first
        if equality_positions and sort_positions:
            if max(equality_positions) > min(sort_positions):
                esr_violations += 1
        
        if equality_positions and range_positions:
            if max(equality_positions) > min(range_positions):
                esr_violations += 1
        
        # Sort should come before range
        if sort_positions and range_positions:
            if max(sort_positions) > min(range_positions):
                esr_violations += 1
        
        # Calculate score (fewer violations = higher score)
        max_violations = 3  # Maximum possible violations
        esr_score = max(0, 100 - (esr_violations / max_violations) * 100)
        
        return int(esr_score)
    
    def _validate_esr_compliance(self, index_structure: List[Tuple[str, int]], 
                                query_field_types: Dict[str, str]) -> List[str]:
        """Validate ESR compliance and return list of violations.
        
        Args:
            index_structure: Index structure as list of (field, direction)
            query_field_types: Field usage types from query
            
        Returns:
            List of ESR violation descriptions
        """
        violations = []
        
        if not index_structure:
            return violations
        
        # Group fields by type
        equality_fields = [f for f, t in query_field_types.items() if t == 'equality']
        sort_fields = [f for f, t in query_field_types.items() if t == 'sort']
        range_fields = [f for f, t in query_field_types.items() if t == 'range']
        
        # Find positions in index
        index_fields = [field for field, _ in index_structure]
        
        # Check ESR rule: Equality -> Sort -> Range
        # Find positions of each field type in the index
        equality_positions = [i for i, field in enumerate(index_fields) if field in equality_fields]
        sort_positions = [i for i, field in enumerate(index_fields) if field in sort_fields]
        range_positions = [i for i, field in enumerate(index_fields) if field in range_fields]
        
        # Check if range fields come before equality fields
        for range_pos in range_positions:
            for eq_pos in equality_positions:
                if range_pos < eq_pos:
                    violations.append(f"Range field '{index_fields[range_pos]}' appears before equality field '{index_fields[eq_pos]}'")
        
        # Check if range fields come before sort fields
        for range_pos in range_positions:
            for sort_pos in sort_positions:
                if range_pos < sort_pos:
                    violations.append(f"Range field '{index_fields[range_pos]}' appears before sort field '{index_fields[sort_pos]}'")
        
        return violations
    
    def _analyze_selectivity(self, query_field_types: Dict[str, str], 
                           stats: Dict, index_structure: List[Tuple[str, int]]) -> Dict:
        """Analyze index selectivity based on query patterns and performance.
        
        Args:
            query_field_types: Field usage types from query
            stats: Query execution statistics
            index_structure: Current index structure
            
        Returns:
            Selectivity analysis with recommendations
        """
        analysis = {
            'selectivity_score': 100,
            'issues': [],
            'recommendations': []
        }
        
        mean_ms = stats.get('mean', 0)
        count = stats.get('count', 0)
        
        # Analyze field types for selectivity
        equality_fields = [f for f, t in query_field_types.items() if t == 'equality']
        range_fields = [f for f, t in query_field_types.items() if t == 'range']
        
        # Simple equality queries should be fast
        if len(equality_fields) == 1 and len(query_field_types) == 1:
            if mean_ms > 500:  # Simple equality should be < 500ms
                analysis['selectivity_score'] = 50
                analysis['issues'].append("Simple equality query is slow - suggests poor field selectivity")
                analysis['recommendations'].append("Consider adding more selective filters or using compound indexes")
        
        # Range queries are inherently less selective
        if range_fields:
            analysis['selectivity_score'] = min(analysis['selectivity_score'], 70)
            analysis['issues'].append(f"Range queries on {', '.join(range_fields)} are less selective")
            analysis['recommendations'].append("Consider adding equality filters before range filters")
        
        # Multiple equality fields should be more selective
        if len(equality_fields) > 1:
            if mean_ms > 1000:  # Multiple equality should be very fast
                analysis['selectivity_score'] = 60
                analysis['issues'].append("Multiple equality filters but still slow - check data distribution")
                analysis['recommendations'].append("Verify field cardinality and data distribution")
        
        # Check if index structure supports selectivity
        if index_structure:
            index_fields = [field for field, _ in index_structure]
            # If equality fields are not at the beginning of index, it's less selective
            for i, (field, _) in enumerate(index_structure):
                if field in equality_fields and i > 0:
                    # Equality field not at start of index
                    analysis['selectivity_score'] = min(analysis['selectivity_score'], 80)
                    analysis['issues'].append(f"Equality field '{field}' not at start of index")
                    analysis['recommendations'].append("Reorder index to put equality fields first")
        
        return analysis