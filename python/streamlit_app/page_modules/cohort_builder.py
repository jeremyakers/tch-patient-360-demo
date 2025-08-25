"""
Cohort Builder Page for TCH Patient 360 PoC

This page provides advanced cohort creation and analysis tools including:
- Visual cohort definition interface
- Natural language cohort criteria
- Complex inclusion/exclusion rules
- Cohort analytics and comparisons
"""

import streamlit as st
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime, date, timedelta
import logging

from services import data_service, cortex_analyst, session_manager, cortex_agents
from components import analytics_widgets
from utils import helpers, validators
import json

logger = logging.getLogger(__name__)

def render():
    """Entry point called by main.py"""
    render_cohort_builder()

def render_cohort_builder():
    """Main entry point for the cohort builder page"""
    
    st.title("👥 Advanced Cohort Builder")
    st.markdown("Create and analyze patient cohorts using sophisticated criteria and natural language")
    
    # Simplified UI: natural language only
    st.divider()

    # Helper to clear prior cohort results from session state
    def _clear_cohort_selection_state() -> None:
        for k in ['cohort_mrns', 'cohort_identifier_is_patient_id', 'cohort_preview_df']:
            try:
                st.session_state.pop(k, None)
            except Exception:
                pass

    # Suggested cohort examples (click to prefill input)
    st.markdown("**Suggested cohorts**")
    examples: list[str] = [
        "Find patients where clinical documentation mentions having pain and there was an Emergency encounter in past 7 days",
        "Pediatric patients aged 5-15 with asthma who had an emergency department visit in the last 6 months and are currently on inhaled corticosteroids",
        "Patients with diabetes whose HbA1c was greater than 9 in the last 90 days",
        "Patients whose clinical notes mention anxiety and who had a cardiology encounter in the past 6 months",
    ]
    cols = st.columns(min(4, len(examples)))
    for i, ex in enumerate(examples):
        with cols[i % len(cols)]:
            if st.button(ex, key=f"cohort_example_{i}"):
                _clear_cohort_selection_state()
                st.session_state['nl_cohort_text_prefill'] = ex
                st.rerun()

    # If a suggestion set a prefill, apply it BEFORE creating the input widget
    if 'nl_cohort_text_prefill' in st.session_state:
        st.session_state['nl_cohort_text'] = st.session_state.pop('nl_cohort_text_prefill')

    # No in-flight disabling; keep UI responsive and results visible on error

    # Single-line input in a form that submits on Enter
    with st.form("nl_cohort_form", enter_to_submit=True, border=False):
        st.text_input(
            "Describe your cohort in plain English:",
            key="nl_cohort_text",
            placeholder=(
                "Example: Find all pediatric patients aged 5-15 with asthma who had an emergency department "
                "visit in the last 6 months and are currently on inhaled corticosteroids"
            ),
        )
        form_submitted = st.form_submit_button("Search Cohort", type="primary")

    def _run_search(criteria_text: str):
        if not criteria_text:
            st.warning("Please enter cohort criteria to search.")
            return
        try:
            mrn_list, debug_payload, used_sql = _get_mrns_via_analyst(criteria_text)
            if mrn_list:
                st.success(f"✅ Found {len(mrn_list)} MRNs via Cortex Analyst")
                if used_sql:
                    with st.expander("Analyst-generated SQL", expanded=False):
                        st.code(used_sql, language="sql")
                session = session_manager.get_session()
                in_list = ",".join(["'" + m.replace("'","''") + "'" for m in mrn_list])
                use_patient_id = any(str(x).startswith('TCH-') for x in mrn_list)
                where_clause = f"patient_id IN ({in_list})" if use_patient_id else f"mrn IN ({in_list})"
                preview_sql = f"""
                SELECT patient_id, mrn, full_name, current_age, gender, risk_category,
                       total_encounters, last_encounter_date
                FROM PRESENTATION.PATIENT_360
                WHERE {where_clause}
                ORDER BY full_name
                """
                preview_df = session.sql(preview_sql).to_pandas()
                st.session_state['cohort_mrns'] = mrn_list
                st.session_state['cohort_identifier_is_patient_id'] = use_patient_id
                st.session_state['cohort_preview_df'] = preview_df
            else:
                # No matches; clear any stale preview so the table doesn't show old results
                _clear_cohort_selection_state()
                st.info("No patients matched this cohort.")
                sql_for_display = used_sql
                if not sql_for_display and debug_payload is not None:
                    try:
                        sql_for_display = cortex_analyst.extract_sql_from_rest_response(debug_payload)
                    except Exception:
                        sql_for_display = None
                if sql_for_display:
                    with st.expander("Analyst-generated SQL", expanded=True):
                        st.code(sql_for_display, language="sql")
                if debug_payload is not None:
                    expl_text, suggestions = _extract_analyst_text_and_suggestions(debug_payload)
                    if expl_text:
                        st.markdown(f"**Assistant explanation:** {expl_text}")
                    if suggestions:
                        # Deduplicate and remove items that are the same as the top Suggested cohorts
                        try:
                            base_set = set(examples)
                        except Exception:
                            base_set = set()
                        dedup_suggestions = list(dict.fromkeys([s for s in suggestions if s and s not in base_set]))
                        if dedup_suggestions:
                            st.markdown("**Suggested alternatives:**")
                        cols = st.columns(min(3, len(dedup_suggestions))) if dedup_suggestions else []
                        for i, s in enumerate(dedup_suggestions):
                            col = cols[i % max(1, len(cols))]
                            with col:
                                if st.button(s, key=f"analyst_sugg_{i}"):
                                    _clear_cohort_selection_state()
                                    st.session_state['nl_cohort_text_prefill'] = s
                                    st.rerun()
                    with st.expander("Analyst raw analysis", expanded=False):
                        st.json(debug_payload)
        except Exception as e:
            st.error(f"Cohort NL parsing failed: {e}")
            # Ensure the UI re-enables the Search button even on unexpected errors
            try:
                st.session_state['nl_search_inflight'] = False
            except Exception:
                pass

    # Run search synchronously on submit
    if form_submitted:
        try:
            with st.spinner("Searching cohort... this can take a little while for complex criteria"):
                _run_search(st.session_state.get('nl_cohort_text'))
        except Exception as e:
            st.error(f"Search failed: {e}")
            try:
                logger.exception("Cohort search failed")
            except Exception:
                pass

    # Optional: further triggers can set 'nl_cohort_text' and call _run_search elsewhere
    
    # Persist cohort members preview if available (shown full width)
    if st.session_state.get('cohort_preview_df') is not None:
        prev_df = st.session_state.get('cohort_preview_df')
        if prev_df is not None and not prev_df.empty:
            st.markdown("**Cohort Members**")
            st.dataframe(prev_df, use_container_width=True, height=280)

    # Action buttons row (Preview removed)
    st.divider()
    col2, col3 = st.columns(2)
    analyze_clicked = False
    
    with col2:
        if st.button("💾 Save Cohort"):
            st.info("Cohort saving will allow you to reuse and share cohort definitions.")
    
    with col3:
        analyze_clicked = st.button("📊 Analyze Cohort")

    # Run analysis outside the narrow column to use full width
    if analyze_clicked:
        try:
                mrns = st.session_state.get('cohort_mrns', [])
                if not mrns:
                    st.warning("No cohort selected. Parse criteria first to build a cohort.")
                else:
                    session = session_manager.get_session()
                    in_list = ",".join(["'" + m.replace("'","''") + "'" for m in mrns])
                    use_patient_id = st.session_state.get('cohort_identifier_is_patient_id', False)
                    where_clause = f"patient_id IN ({in_list})" if use_patient_id else f"mrn IN ({in_list})"
                    cohort_sql = f"""
                    SELECT 
                        COUNT(DISTINCT patient_id) as patients,
                        AVG(current_age) as avg_age,
                        COUNT(CASE WHEN risk_category='HIGH_RISK' THEN 1 END) as high_risk,
                        SUM(total_encounters) as total_encounters
                    FROM PRESENTATION.PATIENT_360
                    WHERE {where_clause}
                    """
                    stats = session.sql(cohort_sql).to_pandas()
                    if not stats.empty:
                        row = stats.iloc[0]
                        s1, s2, s3, s4 = st.columns(4)
                        with s1: st.metric("Patients", int(row.get('PATIENTS', 0)))
                        with s2: st.metric("Avg Age", f"{float(row.get('AVG_AGE', 0)):.1f}")
                        with s3: st.metric("High Risk", int(row.get('HIGH_RISK', 0)))
                        with s4: st.metric("Total Encounters", int(row.get('TOTAL_ENCOUNTERS', 0)))

                    # Additional cohort analytics for TCH
                    st.divider()
                    st.markdown("### 📈 Cohort Analytics")

                        # Payer mix (insurance)
                    try:
                        payer_sql = f"""
                        SELECT PRIMARY_INSURANCE AS INSURANCE, COUNT(*) AS CNT
                        FROM CONFORMED.PATIENT_MASTER
                        WHERE {'patient_id' if use_patient_id else 'mrn'} IN ({in_list})
                        GROUP BY PRIMARY_INSURANCE
                        ORDER BY CNT DESC
                        """
                        payer_df = session.sql(payer_sql).to_pandas()
                        if not payer_df.empty:
                            analytics_widgets.render_chart_widget(
                                payer_df,
                                'bar', 'Payer Mix', x_col='INSURANCE', y_col='CNT', key='cohort_payer_mix'
                            )
                    except Exception:
                        pass

                        # ED visits last 30 days, Inpatient admissions last 6 months, Avg LOS (inpatient)
                        try:
                            util_sql = f"""
                            SELECT
                                SUM(CASE WHEN ENCOUNTER_TYPE='Emergency' AND ENCOUNTER_DATE>=DATEADD('day',-30,CURRENT_DATE()) THEN 1 ELSE 0 END) AS ed_30d,
                                SUM(CASE WHEN ENCOUNTER_TYPE='Inpatient' AND ENCOUNTER_DATE>=DATEADD('month',-6,CURRENT_DATE()) THEN 1 ELSE 0 END) AS ip_6m,
                                AVG(CASE WHEN ENCOUNTER_TYPE='Inpatient' THEN LENGTH_OF_STAY_DAYS END) AS avg_los
                            FROM CONFORMED.ENCOUNTER_SUMMARY
                            WHERE {'patient_id' if use_patient_id else 'mrn'} IN ({in_list})
                            """
                            util_df = session.sql(util_sql).to_pandas()
                            if not util_df.empty:
                                u = util_df.iloc[0]
                                c1, c2, c3 = st.columns(3)
                                with c1: st.metric("ED visits (30d)", int(u.get('ED_30D', 0)))
                                with c2: st.metric("Inpatient admits (6m)", int(u.get('IP_6M', 0)))
                                with c3: st.metric("Avg LOS (days)", f"{float(u.get('AVG_LOS', 0) or 0):.1f}")
                        except Exception:
                            pass

                        # Department utilization last 6 months
                    try:
                        dept_sql = f"""
                        SELECT DEPARTMENT_NAME AS DEPARTMENT, COUNT(*) AS CNT
                        FROM CONFORMED.ENCOUNTER_SUMMARY
                        WHERE {'patient_id' if use_patient_id else 'mrn'} IN ({in_list})
                          AND ENCOUNTER_DATE >= DATEADD('month', -6, CURRENT_DATE())
                        GROUP BY DEPARTMENT_NAME
                        ORDER BY CNT DESC
                        LIMIT 10
                        """
                        dept_df = session.sql(dept_sql).to_pandas()
                        if not dept_df.empty:
                            analytics_widgets.render_chart_widget(
                                dept_df,
                                'bar', 'Top Departments (6 months)', x_col='DEPARTMENT', y_col='CNT', key='cohort_depts'
                            )
                    except Exception:
                        pass

                        # Medication classes for active medications
                    try:
                        meds_sql = f"""
                        SELECT MEDICATION_CLASS, COUNT(*) AS CNT
                        FROM CONFORMED.MEDICATION_FACT
                        WHERE {'patient_id' if use_patient_id else 'mrn'} IN ({in_list})
                          AND (END_DATE IS NULL OR END_DATE >= CURRENT_DATE())
                        GROUP BY MEDICATION_CLASS
                        ORDER BY CNT DESC
                        LIMIT 8
                        """
                        meds_df = session.sql(meds_sql).to_pandas()
                        if not meds_df.empty:
                            analytics_widgets.render_chart_widget(
                                meds_df,
                                'bar', 'Active Medication Classes', x_col='MEDICATION_CLASS', y_col='CNT', key='cohort_meds'
                            )
                    except Exception:
                        pass

                        # Abnormal labs last 90 days
                    try:
                        labs_sql = f"""
                        SELECT 
                            COUNT(*) AS TOTAL,
                            COUNT(CASE WHEN ABNORMAL_FLAG IS NOT NULL AND ABNORMAL_FLAG NOT IN ('Normal','N') THEN 1 END) AS ABNORMAL
                        FROM CONFORMED.LAB_RESULTS_FACT
                        WHERE {'patient_id' if use_patient_id else 'mrn'} IN ({in_list})
                          AND RESULT_DATE >= DATEADD('day', -90, CURRENT_DATE())
                        """
                        labs_df = session.sql(labs_sql).to_pandas()
                        if not labs_df.empty:
                            total = int(labs_df.iloc[0].get('TOTAL', 0) or 0)
                            abnormal = int(labs_df.iloc[0].get('ABNORMAL', 0) or 0)
                            rate = (abnormal / total * 100) if total > 0 else 0.0
                            lc1, lc2, lc3 = st.columns(3)
                            with lc1: st.metric("Lab results (90d)", total)
                            with lc2: st.metric("Abnormal (90d)", abnormal)
                            with lc3: st.metric("Abnormal rate", f"{rate:.1f}%")
                    except Exception:
                        pass

                        # Engagement and cost metrics (from presentation if available)
                    try:
                        fin_sql = f"""
                        SELECT 
                            AVG(COALESCE(PORTAL_LOGINS_LAST_30_DAYS,0)) AS AVG_PORTAL_LOGINS,
                            AVG(COALESCE(AVG_COST_PER_ENCOUNTER,0)) AS AVG_COST_PER_ENCOUNTER,
                            SUM(COALESCE(TOTAL_LIFETIME_CHARGES,0)) AS TOTAL_LIFETIME_CHARGES
                        FROM PRESENTATION.PATIENT_360
                        WHERE {where_clause}
                        """
                        fin_df = session.sql(fin_sql).to_pandas()
                        if not fin_df.empty:
                            f = fin_df.iloc[0]
                            fc1, fc2, fc3 = st.columns(3)
                            with fc1: st.metric("Avg portal logins (30d)", f"{float(f.get('AVG_PORTAL_LOGINS', 0) or 0):.1f}")
                            with fc2: st.metric("Avg cost/encounter", f"${float(f.get('AVG_COST_PER_ENCOUNTER', 0) or 0):,.0f}")
                            with fc3: st.metric("Total lifetime charges", f"${float(f.get('TOTAL_LIFETIME_CHARGES', 0) or 0):,.0f}")
                    except Exception:
                        pass
        except Exception as e:
            st.error(f"Cohort analysis failed: {e}")

def _extract_sql_from_analyst_response(analysis: Any) -> Optional[str]:
    """Extract a SQL string from various possible Analyst response shapes."""
    try:
        if analysis is None:
            return None
        # If response is a string that looks like SQL (no code fences)
        if isinstance(analysis, str):
            text = analysis.strip()
            if text.upper().startswith("SELECT"):
                return text
            # try JSON load
            try:
                import json
                obj = json.loads(text)
                analysis = obj
            except Exception:
                return None
        # Direct keys
        for key in [
            'sql','SQL','generated_sql','generatedSql','executableSql','sqlStatement','sql_code'
        ]:
            val = analysis.get(key) if isinstance(analysis, dict) else None
            if isinstance(val, str) and val.strip().upper().startswith("SELECT"):
                return val.strip()
        # Nested structures
        candidates = []
        if isinstance(analysis, dict):
            for k in ['response','result','results','data','analysis','answer']:
                v = analysis.get(k)
                if isinstance(v, dict):
                    candidates.append(v)
        for obj in candidates:
            for key in ['sql','SQL','sql_code']:
                v = obj.get(key)
                if isinstance(v, str) and v.strip().upper().startswith('SELECT'):
                    return v.strip()
        # Lists of statements
        for list_key in ['statements','queries','sqls']:
            lst = analysis.get(list_key) if isinstance(analysis, dict) else None
            if isinstance(lst, list):
                for item in lst:
                    if isinstance(item, str) and item.strip().upper().startswith('SELECT'):
                        return item.strip()
                    if isinstance(item, dict):
                        for key in ['sql','SQL']:
                            v = item.get(key)
                            if isinstance(v, str) and v.strip().upper().startswith('SELECT'):
                                return v.strip()
        return None
    except Exception:
        return None

def _get_mrns_via_agents(criteria_text: str) -> tuple[list[str], Optional[Any], Optional[str]]:
    """Query Cortex Agents for a list of MRNs given NL criteria.
    Returns (mrns, raw_response, used_sql)."""
    # Compose a strict instruction for the agent
    user_msg = (
        "Return only patients that match this cohort definition: " + criteria_text + ". "
        "Use the semantic model. Your final output must be JSON with the exact shape: {\"mrns\": [\"<MRN>\", ...]} "
        "with 0 or more MRNs. Do not include any other keys or text."
    )
    agent = cortex_agents
    response = agent.send_message(user_msg)
    # Try to extract MRNs directly from JSON payloads in the streaming events
    mrns = _extract_mrns_from_agent_response(response)
    used_sql = None
    if not mrns:
        # Try to parse for SQL and execute to obtain MRNs
        text, sql_query, _ = agent.process_agent_response(response)
        if sql_query:
            used_sql = sql_query
            try:
                result = agent.execute_sql_query(sql_query)
                if result is not None:
                    df = result.to_pandas()
                    if not df.empty:
                        if 'MRN' in df.columns:
                            mrns = [str(x) for x in df['MRN'].dropna().unique().tolist()]
                        elif 'PATIENT_ID' in df.columns:
                            mrns = [str(x) for x in df['PATIENT_ID'].dropna().unique().tolist()]
                        else:
                            # Attempt to find a likely MRN column
                            for c in df.columns:
                                if c.upper() in ('MRN','PATIENT_MRN','MEDICAL_RECORD_NUMBER'):
                                    mrns = [str(x) for x in df[c].dropna().unique().tolist()]
                                    break
                            # Handle case where the query returns OBJECT_CONSTRUCT with 'mrns' key
                            if not mrns and df.shape[1] == 1:
                                first_val = df.iloc[0, 0]
                                try:
                                    import json as _json
                                    obj = first_val if isinstance(first_val, dict) else _json.loads(str(first_val))
                                    if isinstance(obj, dict) and 'mrns' in obj and isinstance(obj['mrns'], list):
                                        mrns = [str(x) for x in obj['mrns'] if x]
                                except Exception:
                                    pass
            except Exception:
                pass
    return mrns or [], response, used_sql

def _get_mrns_via_analyst(criteria_text: str) -> tuple[list[str], Optional[Any], Optional[str]]:
    """Use Cortex Analyst REST API to produce an MRN list. Returns (mrns, raw_analysis, used_sql)."""
    try:
        # Instruction: concise, generalized guidance so Analyst reliably picks structured sources
        prompt = (
            "Return only medical record numbers (MRNs) for patients that match this cohort definition: "
            + criteria_text
            + " Your response MUST be pure SQL that when executed returns a single column named MRN. "
            + "Use presentation tables and prefer structured data: patient_360 (age/demographics), diagnosis_analytics (ICD-10), "
            + "encounter_analytics (encounter_type/date/department), medication_analytics (is_active, route, start/end dates), lab_results_analytics (test values/dates). "
            + "Only use AI functions on clinical_documentation when the question explicitly asks to search notes. "
            + "For time windows, use DATEADD functions on date columns; do not approximate. "
            + "Do not include any prose, JSON, or code fences—output only SQL that yields a column MRN."
        )
        analysis = cortex_analyst.ask_analyst_rest(prompt, stream=False)

        # Extract SQL and execute it. Guard against empty/blank SQL to avoid parsing errors
        sql_query = cortex_analyst.extract_sql_from_rest_response(analysis) or _extract_sql_from_analyst_response(analysis)
        if not sql_query or not str(sql_query).strip():
            return [], analysis, None

        try:
            session = session_manager.get_session()
            clean_sql = str(sql_query).strip().rstrip(';')
            df = session.sql(clean_sql).to_pandas()
            mrns: list[str] = []
            if not df.empty:
                cols_upper = {c.upper(): c for c in df.columns}
                if 'MRN' in cols_upper:
                    mrns = [str(x) for x in df[cols_upper['MRN']].dropna().unique().tolist()]
                elif df.shape[1] == 1:
                    col = df.columns[0]
                    mrns = [str(x) for x in df[col].dropna().unique().tolist()]
            return list(dict.fromkeys(mrns)), analysis, clean_sql
        except Exception as _e:
            return [], {"analysis": analysis, "sql_error": str(_e)}, str(sql_query).strip().rstrip(';')
    except Exception as e:
        return [], {'error': str(e)}, None

def _extract_analyst_text_and_suggestions(rest_response: Any) -> tuple[Optional[str], list[str]]:
    """From Analyst REST response, extract the main explanation text and suggestions list.
    Returns (text, suggestions)."""
    try:
        data = rest_response
        if isinstance(rest_response, dict) and 'content' in rest_response and isinstance(rest_response['content'], str):
            import json as _json
            try:
                data = _json.loads(rest_response['content'])
            except Exception:
                data = rest_response
        message = (data or {}).get('message', {}) if isinstance(data, dict) else {}
        explanation = None
        suggestions: list[str] = []
        for item in message.get('content', []) or []:
            if isinstance(item, dict) and item.get('type') == 'text':
                txt = item.get('text')
                if isinstance(txt, str) and txt.strip():
                    explanation = txt.strip()
            if isinstance(item, dict) and item.get('type') == 'suggestions':
                lst = item.get('suggestions', [])
                if isinstance(lst, list):
                    suggestions = [str(x) for x in lst if x]
        return explanation, suggestions
    except Exception:
        return None, []

def _extract_mrns_from_agent_response(response: Any) -> list[str]:
    """Best-effort extraction of MRNs from various agent response formats."""
    mrns: list[str] = []
    try:
        # Apply a relevance score threshold for Cortex Search results
        try:
            import streamlit as _st
            score_threshold = float(_st.session_state.get('cortex_search_score_threshold', 0.6))
        except Exception:
            score_threshold = 0.6
        # If already JSON with 'mrns'
        if isinstance(response, dict) and 'mrns' in response and isinstance(response['mrns'], list):
            return [str(x) for x in response['mrns'] if x]
        # If response is dict with 'content' string containing events
        events = None
        if isinstance(response, dict) and 'content' in response:
            content = response['content']
            if isinstance(content, str):
                import json as _json
                try:
                    events = _json.loads(content)
                except Exception:
                    events = None
            elif isinstance(content, list):
                events = content
        elif isinstance(response, list):
            events = response
        # Walk events for JSON tool_results containing mrns or rows
        if isinstance(events, list):
            for ev in events:
                data = ev.get('data', {}) if isinstance(ev, dict) else {}
                delta = data.get('delta', {})
                for content_item in delta.get('content', []):
                    if content_item.get('type') == 'tool_results':
                        for item in content_item.get('tool_results', {}).get('content', []):
                            if item.get('type') == 'json':
                                js = item.get('json', {})
                                if isinstance(js, dict):
                                    # direct mrns
                                    if 'mrns' in js and isinstance(js['mrns'], list):
                                        mrns.extend([str(x) for x in js['mrns'] if x])
                                    # searchResults (from Cortex Search)
                                    search_results = js.get('searchResults')
                                    if isinstance(search_results, list):
                                        for sr in search_results:
                                            if isinstance(sr, dict):
                                                # Use citations for exact matching docs: extract MRN from title_column 'MRN' when available
                                                # Prefer explicit MRN field, then title_column surfaced as doc_title
                                                v = sr.get('MRN') or sr.get('mrn')
                                                if not v:
                                                    title_val = sr.get('doc_title') or sr.get('title')
                                                    if isinstance(title_val, str) and title_val.strip():
                                                        # Common case: title set to the MRN string
                                                        v = title_val.strip()
                                                if not v:
                                                    # Fallback: parse typical "MRN: <value>" pattern from snippet text
                                                    txt = sr.get('text') or ''
                                                    import re as _re
                                                    m = _re.search(r"\bMRN[:\s]+([A-Za-z0-9-]+)", str(txt))
                                                    if m:
                                                        v = m.group(1)
                                                if v:
                                                    mrns.append(str(v))
                                    # tabular rows
                                    rows = js.get('rows') or js.get('data') or js.get('results')
                                    if isinstance(rows, list):
                                        for r in rows:
                                            if isinstance(r, dict):
                                                for k, v in r.items():
                                                    if k.upper() in ('MRN','PATIENT_MRN','MEDICAL_RECORD_NUMBER') and v:
                                                        mrns.append(str(v))
        return list(dict.fromkeys(mrns))
    except Exception:
        return []

def _calculate_demo_cohort_size(age_min: int, age_max: int, gender: List[str], diagnosis: str, department: List[str]) -> int:
    """Calculate estimated cohort size based on demo criteria"""
    base_population = 47392  # Total TCH patient population
    
    # Age factor
    age_factor = (age_max - age_min + 1) / 22  # 22 age groups (0-21)
    
    # Gender factor
    gender_factor = len(gender) / 2 if gender else 1
    
    # Diagnosis factor
    diagnosis_factor = 0.1 if diagnosis else 1  # Specific conditions reduce population
    
    # Department factor
    dept_factor = 0.3 if department else 1  # Department filtering reduces population
    
    estimated = int(base_population * age_factor * gender_factor * diagnosis_factor * dept_factor)
    
    # Add some randomness for realism
    import random
    variation = random.uniform(0.8, 1.2)
    
    return max(1, int(estimated * variation))

def _parse_demo_criteria(criteria: str) -> List[str]:
    """Parse natural language criteria into structured conditions"""
    parsed = []
    criteria_lower = criteria.lower()
    
    # Age parsing
    import re
    age_match = re.search(r'aged?\s+(\d+)[-\s]*(?:to|-)?\s*(\d+)', criteria_lower)
    if age_match:
        parsed.append(f"Age between {age_match.group(1)} and {age_match.group(2)} years")
    
    # Condition parsing
    conditions = ["asthma", "diabetes", "cardiac", "pneumonia", "bronchitis"]
    for condition in conditions:
        if condition in criteria_lower:
            parsed.append(f"Diagnosis includes '{condition.title()}'")
    
    # Time period parsing
    if "last 6 months" in criteria_lower:
        parsed.append("Encounters within last 6 months")
    elif "last year" in criteria_lower:
        parsed.append("Encounters within last year")
    
    # Department parsing
    if "emergency" in criteria_lower:
        parsed.append("Emergency department encounters")
    
    # Medication parsing
    if "medication" in criteria_lower or "inhaled" in criteria_lower:
        parsed.append("Active medication criteria specified")
    
    if not parsed:
        parsed.append("Custom criteria requiring advanced parsing")
    
    return parsed