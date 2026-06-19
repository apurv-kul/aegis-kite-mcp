"""
Project Aegis — src/agents/state.py
=====================================
Central memory ledger for the LangGraph multi-agent orchestrator.

Defines ``AgentState`` — the single TypedDict that flows through every node
in the graph and acts as the agent's short-term working memory during one
complete evaluation → execution loop.

Imports are bound strictly to the canonical contract files:
  src/contracts/primitives.py  — OptionType, TransactionType, StrategyType, TradeInstrument
  src/contracts/research.py    — StrikeLeg, TradeThesis
  src/contracts/risk.py        — RiskAssessment
  src/contracts/execution.py   — ExecutionOrder
  src/contracts/ingestion.py   — IngestionPayload

Architecture position
---------------------

  ┌─────────────────────────────────────────────────────────────────────────┐
  │                     LangGraph Orchestration Graph                       │
  │                                                                         │
  │  [START]                                                                │
  │     │                                                                   │
  │     ▼                                                                   │
  │  DiscoveryNode          Reads:  —                                       │
  │  (MCP: get_options_     Writes: authorized_universe                     │
  │   chain, market scan)           current_options_chain                   │
  │                                 market_snapshot                         │
  │                                 messages ← appends                      │
  │     │                                                                   │
  │     ▼                                                                   │
  │  StrategyNode           Reads:  current_options_chain, market_snapshot  │
  │  (Research Agent LLM)   Writes: draft_thesis                            │
  │                                 thesis_iteration (increments)           │
  │                                 messages ← appends                      │
  │     │                                                                   │
  │     ▼                                                                   │
  │  EventRiskGate          Reads:  draft_thesis                            │
  │  (calendar check)       Writes: event_risk_cleared                      │
  │                                 event_risk_detail                       │
  │     │                                                                   │
  │    [event_risk_cleared == False] ──► [END: abort]                       │
  │     │                                                                   │
  │     ▼                                                                   │
  │  HistoricalContextGate  Reads:  draft_thesis, authorized_universe       │
  │  (Qdrant + KG lookup)   Writes: historical_context                      │
  │                                 historical_win_rate                     │
  │                                 historical_avg_pnl_inr                  │
  │     │                                                                   │
  │     ▼                                                                   │
  │  RiskEngine             Reads:  draft_thesis, event_risk_cleared,       │
  │  (guardrail checks)             historical_context                      │
  │                         Writes: risk_assessment                         │
  │     │                                                                   │
  │    [risk_assessment.is_approved == False] ──► [END: rejected]           │
  │     │                                                                   │
  │     ▼                                                                   │
  │  ExecutionNode          Reads:  risk_assessment, draft_thesis           │
  │  (MCP: place_order)     Writes: execution_order                         │
  │                                 messages ← appends                      │
  │     │                                                                   │
  │     ▼                                                                   │
  │  PostTradeNode          Reads:  execution_order, draft_thesis,          │
  │  (LLM post-mortem)              risk_assessment                         │
  │                         Writes: messages ← appends                      │
  │                                 (triggers KG + Qdrant update via MCP)  │
  │  [END]                                                                  │
  └─────────────────────────────────────────────────────────────────────────┘

Reducer semantics
-----------------
* ``messages``      → ``operator.add``  (append-only; never overwrites)
* All other fields  → last-write-wins   (standard LangGraph default)

Nullability convention
-----------------------
Fields unavailable at graph start are typed ``T | None`` with default None.
Nodes that depend on a field MUST guard with:
    if state["field"] is None: raise / route-away
rather than relying on silent AttributeErrors.

None vs empty-list distinction
-------------------------------
For list fields that are gate outputs (e.g. historical_context):
  None  = gate has not run yet
  []    = gate ran, found nothing  (valid state; downstream nodes handle it)
This distinction is critical for crash-recovery on graph checkpoint resume.
"""

from __future__ import annotations

import operator
from datetime import datetime, timezone
from typing import Annotated, Any, Sequence, TypedDict

from langchain_core.messages import BaseMessage

# ── Canonical contract imports ────────────────────────────────────────────────
# These are the ONLY allowed import sources.  Do not import from the
# locally-generated research.py / execution.py written during earlier phases;
# those are superseded by the uploaded canonical versions below.
from src.contracts.execution import ExecutionOrder
from src.contracts.research import TradeThesis
from src.contracts.risk import RiskAssessment


# ─────────────────────────────────────────────────────────────────────────────
# Supporting TypedDicts
# Typed sub-dicts used as field payloads within AgentState.
# TypedDict (not Pydantic) keeps them LangGraph-serialisable without requiring
# model_dump() calls inside every node.
# ─────────────────────────────────────────────────────────────────────────────

class HistoricalAnalogue(TypedDict, total=False):
    """
    A single historical trade analogue retrieved from the Qdrant vector store
    and enriched with Neo4j knowledge-graph outcome data.

    Populated by HistoricalContextGate; consumed by RiskEngine and StrategyNode.

    Fields
    ------
    strategy_type : str
        StrategyType enum value from the historical trade
        (e.g. "Bear Call Spread", "Iron Condor").
    similarity_score : float
        Cosine similarity [0.0, 1.0] between the current market-state embedding
        and this historical state embedding.  Sorted descending in the list.
    outcome_pnl_inr : float
        Realised P&L in ₹ for the historical trade.
    outcome_win : bool
        True if the trade was profitable (pnl > 0) after all costs.
    confidence_at_entry : int
        TradeThesis.confidence value at the time of the historical trade.
        Used to calibrate whether high-confidence calls actually outperform.
    stop_loss_pct : float
        stop_loss_pct from the historical thesis — used to compare with the
        current draft_thesis for risk-profile similarity scoring.
    target_profit_pct : float
        target_profit_pct from the historical thesis.
    trade_date : str | None
        ISO date string of the historical trade entry (from trading_journal).
    exit_reason : str | None
        Human-readable exit reason logged at trade close.
    notes : str | None
        Post-mortem LLM notes from the historical trade.
    """
    strategy_type:       str
    similarity_score:    float
    outcome_pnl_inr:     float
    outcome_win:         bool
    confidence_at_entry: int
    stop_loss_pct:       float
    target_profit_pct:   float
    trade_date:          str | None
    exit_reason:         str | None
    notes:               str | None


class EventRiskDetail(TypedDict, total=False):
    """
    Structured companion to the ``event_risk_cleared`` boolean flag.

    Populated by EventRiskGate when a blocking event is detected
    (i.e. when event_risk_cleared is set to False).
    Empty / None when event_risk_cleared is True.

    The Risk Engine checks event_risk_cleared first; this dict supplies
    the human-readable and machine-readable context for the abort reason
    and for the trading_journal session log.

    Fields
    ------
    event_type : str
        Category: 'RBI_MPC', 'EARNINGS', 'BUDGET', 'FNO_EXPIRY',
        'GLOBAL_DATA_RELEASE', 'INDEX_REBALANCE'.
    event_name : str
        Human-readable description, e.g. "RBI MPC Rate Decision – Jun 2024".
    event_date : str
        ISO date of the event.
    days_until_event : int
        Calendar days from today to the event date.
    within_blackout_window : bool
        True if the event falls within the configured T-1 to T+1 blackout.
    recommendation : str
        Mitigation suggestion: 'ABORT', 'REDUCE_SIZE', 'HEDGE_REQUIRED'.
    """
    event_type:             str
    event_name:             str
    event_date:             str
    days_until_event:       int
    within_blackout_window: bool
    recommendation:         str


class MarketSnapshot(TypedDict, total=False):
    """
    Point-in-time macro snapshot captured by DiscoveryNode.

    Stored as context for StrategyNode (regime classification) and
    RiskEngine (VIX-based guardrail checks).

    Fields
    ------
    captured_at : str
        ISO UTC timestamp of the snapshot.
    india_vix : float | None
        India VIX reading.  RiskEngine blocks short-premium strategies if > 20.
    nifty50_spot : float | None
        NIFTY 50 index spot price.
    banknifty_spot : float | None
        BANK NIFTY index spot price.
    fii_net_inr_cr : float | None
        FII net buy/sell in ₹ crore (positive = net buy).
    dii_net_inr_cr : float | None
        DII net buy/sell in ₹ crore.
    advance_decline_ratio : float | None
        NSE advance / decline ratio (market breadth indicator).
    us_futures_change_pct : float | None
        S&P 500 / Dow futures % change (global cue for gap-risk assessment).
    crude_oil_usd : float | None
        Brent crude in USD/barrel (macro input for energy sector theses).
    usdinr : float | None
        USD/INR spot rate.
    regime : str | None
        Plain-text regime label: 'TRENDING_BULL', 'TRENDING_BEAR',
        'RANGING', 'HIGH_VOL', 'EVENT_DRIVEN'.
        Computed by DiscoveryNode; used by StrategyNode to constrain
        which StrategyType values are permissible in the draft_thesis.
    """
    captured_at:            str
    india_vix:              float | None
    nifty50_spot:           float | None
    banknifty_spot:         float | None
    fii_net_inr_cr:         float | None
    dii_net_inr_cr:         float | None
    advance_decline_ratio:  float | None
    us_futures_change_pct:  float | None
    crude_oil_usd:          float | None
    usdinr:                 float | None
    regime:                 str   | None


# ─────────────────────────────────────────────────────────────────────────────
# AgentState
# ─────────────────────────────────────────────────────────────────────────────

class AgentState(TypedDict):
    """
    The Global State for the Project Aegis Deep Research Agent.

    Strictly defines the AI's short-term working memory during a single
    evaluation loop — from instrument discovery through to execution.

    Every node receives this full state dict and returns a *partial* dict
    containing only the fields it updates.  LangGraph merges the partial
    update back into the canonical state using the reducer defined per field.

    Contract alignment
    ------------------
    All Pydantic-typed fields reference the canonical uploaded contracts:
      draft_thesis     → src.contracts.research.TradeThesis
      risk_assessment  → src.contracts.risk.RiskAssessment
      execution_order  → src.contracts.execution.ExecutionOrder

    Field groups
    ------------
    1. Message history      — LLM conversation turns (append-only)
    2. Session metadata     — loop identity, timestamps, trigger source
    3. Market structure     — raw data from DiscoveryNode MCP calls
    4. Core strategy        — draft thesis from StrategyNode
    5. Enterprise guardrails— event gate + historical context locks
    6. Final verdict        — RiskEngine output
    7. Execution payload    — live order state
    8. Observability        — error context, node trace, abort signals
    """

    # ─────────────────────────────────────────────────────────────────────────
    # 1. Message History
    #
    # Append-only LangChain BaseMessage sequence.
    # ``operator.add`` reducer ensures node updates APPEND to this list
    # rather than overwriting it.  The full thread is passed to every LLM
    # node so context is preserved across the discovery → strategy →
    # risk → execution flow within a single session.
    #
    # Contains: HumanMessage (trigger / user intent),
    #           AIMessage (LLM reasoning outputs),
    #           ToolMessage (MCP tool call results),
    #           SystemMessage (per-node instructions injected at runtime).
    # ─────────────────────────────────────────────────────────────────────────

    messages: Annotated[Sequence[BaseMessage], operator.add]

    # ─────────────────────────────────────────────────────────────────────────
    # 2. Session Metadata
    # ─────────────────────────────────────────────────────────────────────────

    session_id: str | None
    """
    Unique identifier for this agent evaluation loop.
    Format: 'AEGIS-{YYYYMMDD}-{uuid4[:6].upper()}'.
    Auto-generated by initial_state() if not supplied.
    Used as the primary key in the trading_journal session log.
    """

    session_started_at: str | None
    """ISO UTC timestamp when this evaluation loop was initiated."""

    evaluation_trigger: str | None
    """
    What initiated this evaluation loop.
    Values: 'SCHEDULED' (market-open cron), 'SIGNAL' (inbound market signal),
            'MANUAL' (operator-triggered), 'RETRY' (post-rejection retry).
    """

    # ─────────────────────────────────────────────────────────────────────────
    # 3. Market Structure — populated by DiscoveryNode via MCP tool calls
    # ─────────────────────────────────────────────────────────────────────────

    authorized_universe: list[str]
    """
    Screened set of underlying symbols approved for this evaluation session.

    Populated by DiscoveryNode after sector momentum, volume, news-catalyst,
    and earnings-proximity filters are applied to the full F&O universe.

    Format: list of underlying symbols (not full tradingsymbols), e.g.
        ['NIFTY', 'BANKNIFTY', 'HDFCBANK']

    StrategyNode selects ONE underlying from this list for the draft_thesis
    (stored in selected_underlying).  An empty list signals no tradeable
    opportunity — the graph sets abort_reason='NO_OPPORTUNITY' and exits.
    """

    selected_underlying: str | None
    """
    The single underlying chosen from authorized_universe for this thesis.
    Set by DiscoveryNode or StrategyNode after ranking the screened universe.
    E.g. 'NIFTY', 'HDFCBANK'.  None until selection is made.
    """

    selected_expiry: str | None
    """
    ISO date string of the target expiry for the strategy.
    E.g. '2024-06-27'.
    Set by StrategyNode based on DTE preferences and options chain data.
    None until StrategyNode runs.
    """

    current_options_chain: dict[str, Any] | None
    """
    Raw output from the ``get_options_chain`` MCP tool for the selected
    underlying and expiry.  Schema matches kite_mcp._assemble_chain():

    {
        "status":      "ok",
        "underlying":  "NIFTY",
        "expiry":      "2024-06-27",
        "spot_price":  23150.40,
        "atm_strike":  23000.0,
        "max_pain":    22800.0,
        "pcr_oi":      0.92,
        "pcr_volume":  0.88,
        "total_ce_oi": 12500000,
        "total_pe_oi": 11500000,
        "chain":       [ {"strike": 23000, "call": {...}, "put": {...}}, ... ]
    }

    None until DiscoveryNode calls get_options_chain.
    StrategyNode reads this to select strikes for the StrikeLeg list in
    the TradeThesis and to validate IV levels against the market_snapshot.
    """

    market_snapshot: MarketSnapshot | None
    """
    Point-in-time macro snapshot captured by DiscoveryNode.
    Includes India VIX, index spots, FII/DII flows, and computed regime label.
    None until DiscoveryNode macro scan completes.
    """

    # ─────────────────────────────────────────────────────────────────────────
    # 4. Core Strategy — populated by StrategyNode (the Research Agent LLM)
    # ─────────────────────────────────────────────────────────────────────────

    draft_thesis: TradeThesis | None
    """
    The Research Agent's complete trade proposal.
    Type: src.contracts.research.TradeThesis (canonical, frozen Pydantic model).

    Fields the RiskEngine and downstream nodes read:
      .strategy_type          → StrategyType enum value
      .primary_instrument     → TradeInstrument (underlying, expiry, strike, OptionType)
      .strike_legs            → list[StrikeLeg] — all option legs
      .expected_entry_premium → net premium cost/receipt in ₹
      .target_profit_pct      → profit target as % of expected_entry_premium
      .stop_loss_pct          → stop as % of expected_entry_premium
      .confidence             → int [0, 100]; < 60 = auto-reject in RiskEngine
      .rationale              → LLM explanation (max 1024 chars)

    None until StrategyNode produces its first thesis.
    Overwritten (last-write-wins) on each retry iteration.
    Maximum thesis_iteration = 3 before the graph routes to abort.
    """

    thesis_iteration: int | None
    """
    Counter tracking how many draft theses have been generated this session.
    Initialised to 0 by initial_state().  Incremented by StrategyNode on each
    attempt (including the first).  When thesis_iteration reaches 3, the graph
    supervisor routes to END with abort_reason='MAX_ITERATIONS'.
    """

    # ─────────────────────────────────────────────────────────────────────────
    # 5. Enterprise Guardrails
    #    Hard locks between thesis generation and execution.
    #    Both gates MUST be satisfied before RiskEngine runs.
    # ─────────────────────────────────────────────────────────────────────────

    event_risk_cleared: bool | None
    """
    Hard boolean lock set by EventRiskGate.

    None   → EventRiskGate has not run yet.
             RiskEngine MUST treat this identically to False (abort).
    True   → No conflicting events detected; thesis may proceed to RiskEngine.
    False  → A blackout-window event was detected; graph aborts immediately.

    Blackout conditions that set this to False:
      - RBI MPC meeting within T-1 to T+1 of strategy expiry date
      - Quarterly earnings for the selected underlying within T-1 to T+1
      - Union Budget day (full session blackout, all underlyings)
      - NSE F&O expiry day (no new position opens; exits only)
      - Major global data releases (US CPI, FOMC) within 6 hours of market open

    The RiskEngine MUST check:
        if state["event_risk_cleared"] is not True:
            raise RuntimeError("EventRiskGate not cleared — cannot proceed")
    This prevents any accidental bypass when the gate is None (graph restart).
    """

    event_risk_detail: EventRiskDetail | None
    """
    Structured companion to event_risk_cleared.
    Populated when event_risk_cleared is False; None when True.
    Provides the specific event name, date, and mitigation recommendation
    for the trading_journal abort log entry.
    """

    historical_context: list[HistoricalAnalogue] | None
    """
    Top-K historical trade analogues retrieved by HistoricalContextGate
    from the Qdrant vector store, enriched with outcome data from Neo4j.

    None  → HistoricalContextGate has not run yet.
    []    → Gate ran; zero analogues found above similarity threshold.
            Downstream behaviour: RiskEngine forces draft_thesis.confidence
            to be treated as ≤ 60 regardless of its actual value (unknown
            territory — reduce confidence automatically).
    [...] → List of HistoricalAnalogue dicts sorted by similarity_score desc.

    Max K = 10 analogues (configured in HistoricalContextGate).
    RiskEngine cross-checks historical_win_rate against the agent's
    implicit win expectation to catch overconfident theses.
    """

    historical_win_rate: float | None
    """
    Aggregate win rate across all historical_context analogues.
    = count(outcome_win == True) / len(historical_context)

    Computed by HistoricalContextGate and stored here to avoid recomputation
    in RiskEngine.

    None if historical_context is None (gate not run) or [] (no analogues).
    0.0 is a valid value meaning all analogues were losers.
    """

    historical_avg_pnl_inr: float | None
    """
    Mean realised P&L in ₹ across historical_context analogues.
    Provides magnitude context alongside win rate.

    Interpretation:
      Positive → historically profitable setups in similar conditions.
      Negative → historically losing setups; strong signal to abort or
                 reduce size to minimum 1 lot.
    None if historical_context is None or empty.
    """

    # ─────────────────────────────────────────────────────────────────────────
    # 6. Final Verdict — populated by RiskEngine
    # ─────────────────────────────────────────────────────────────────────────

    risk_assessment: RiskAssessment | None
    """
    The Risk Engine's complete evaluation of draft_thesis.
    Type: src.contracts.risk.RiskAssessment (canonical, frozen Pydantic model).

    Fields the ExecutionNode reads:
      .is_approved          → True only if ALL guardrails pass
      .max_calculated_loss  → ₹ max loss (used to size execution_order quantity)
      .margin_required      → ₹ SPAN margin (checked against available margin)
      .tripped_risk_rules   → list[str] — human-readable failed rule names
      .risk_engine_version  → semver string for audit trail

    Graph routing after RiskEngine:
      is_approved == True  → ExecutionNode
      is_approved == False → PostTradeNode (log rejection to trading_journal)

    None until RiskEngine node runs.
    RiskEngine will not run unless event_risk_cleared is True AND
    historical_context is not None.
    """

    # ─────────────────────────────────────────────────────────────────────────
    # 7. Execution Payload — populated by ExecutionNode
    #    Set only when risk_assessment.is_approved == True.
    # ─────────────────────────────────────────────────────────────────────────

    execution_order: ExecutionOrder | None
    """
    The concrete order submitted to Kite via the MCP place_order tool.
    Type: src.contracts.execution.ExecutionOrder (canonical, frozen Pydantic model).

    Fields set by ExecutionNode from the approved RiskAssessment + TradeThesis:
      .instrument       → TradeInstrument from draft_thesis.primary_instrument
      .transaction_type → TransactionType (BUY/SELL for the primary leg)
      .order_type       → LIMIT (default) or MARKET for emergency exits
      .quantity         → lots × lot_size, adjusted for risk_assessment constraints
      .lot_size         → from TradeInstrument / NFO instrument master
      .price            → limit price (mid of bid/ask at submission time)
      .algo_id          → 'AEGIS-{session_id[:6]}' format (6–20 chars, [A-Z0-9_-])

    None until ExecutionNode runs.
    The double-execution guard in is_ready_for_execution() ensures ExecutionNode
    is skipped on graph replay if execution_order is already set.

    PostTradeNode reads this to log the execution result to trading_journal.
    The Execution Monitor (separate process) reads algo_id to track live fills
    from the Kite order stream.
    """

    # ─────────────────────────────────────────────────────────────────────────
    # 8. Observability & Audit
    # ─────────────────────────────────────────────────────────────────────────

    last_error: str | None
    """
    Most recent error message from any node.  Overwritten on each new error.
    Used by the supervisor router to decide whether to retry or abort.
    None when the graph is running without errors.
    """

    node_trace: list[str] | None
    """
    Ordered list of node names that have executed in this session.
    Example: ['DiscoveryNode', 'StrategyNode', 'EventRiskGate', 'RiskEngine']
    Appended to by each node.  Persisted to trading_journal as the execution
    audit trail.  Distinct from LangGraph's internal checkpointing.
    """

    abort_reason: str | None
    """
    Set when the graph exits without placing an order.  Possible values:
      'NO_OPPORTUNITY'     — authorized_universe is empty after screening
      'EVENT_RISK_BLOCKED' — event_risk_cleared == False
      'RISK_REJECTED'      — risk_assessment.is_approved == False
      'MAX_ITERATIONS'     — thesis_iteration reached limit (3)
      'LOW_CONFIDENCE'     — draft_thesis.confidence < 60 on all iterations
      'SESSION_ERROR'      — unhandled exception in a node (last_error is set)
      'MANUAL_ABORT'       — operator-initiated stop signal

    None when the graph completes normally (execution attempted).
    Persisted to trading_journal for session-level performance attribution.
    """


# ─────────────────────────────────────────────────────────────────────────────
# Default factory
# ─────────────────────────────────────────────────────────────────────────────

def initial_state(
    session_id:         str | None = None,
    evaluation_trigger: str        = "MANUAL",
) -> AgentState:
    """
    Create a fully initialised AgentState for a new evaluation loop.

    All optional fields are set to None.  All list fields are set to their
    correct empty sentinel:
      authorized_universe → []   (DiscoveryNode populates this)
      node_trace          → []   (each node appends its name)
      messages            → []   (first node appends its SystemMessage)

    Parameters
    ----------
    session_id : str | None
        Explicit session ID.  Auto-generated as 'AEGIS-{YYYYMMDD}-{6-hex}'
        if not supplied.
    evaluation_trigger : str
        What is initiating this loop: 'SCHEDULED', 'SIGNAL', 'MANUAL', 'RETRY'.

    Returns
    -------
    AgentState
        Ready to pass directly to ``graph.invoke()``.

    Usage
    -----
    ::

        from src.agents.state import initial_state
        from langchain_core.messages import HumanMessage

        state = initial_state(evaluation_trigger="SCHEDULED")
        state["messages"] = [HumanMessage(content="Begin daily F&O scan.")]
        result = await graph.ainvoke(state)
    """
    import uuid as _uuid

    _sid = session_id or (
        f"AEGIS-{datetime.now(timezone.utc).strftime('%Y%m%d')}-"
        f"{_uuid.uuid4().hex[:6].upper()}"
    )

    return AgentState(
        # 1. Messages — empty; first node appends its SystemMessage
        messages=[],

        # 2. Session metadata
        session_id=_sid,
        session_started_at=datetime.now(timezone.utc).isoformat(),
        evaluation_trigger=evaluation_trigger,

        # 3. Market structure
        authorized_universe=[],
        selected_underlying=None,
        selected_expiry=None,
        current_options_chain=None,
        market_snapshot=None,

        # 4. Core strategy
        draft_thesis=None,
        thesis_iteration=0,

        # 5. Enterprise guardrails
        # None = not yet evaluated (distinct from False = evaluated and blocked)
        event_risk_cleared=None,
        event_risk_detail=None,
        historical_context=None,
        historical_win_rate=None,
        historical_avg_pnl_inr=None,

        # 6. Final verdict
        risk_assessment=None,

        # 7. Execution payload
        execution_order=None,

        # 8. Observability
        last_error=None,
        node_trace=[],
        abort_reason=None,
    )


# ─────────────────────────────────────────────────────────────────────────────
# State inspection utilities
# ─────────────────────────────────────────────────────────────────────────────

def state_summary(state: AgentState) -> dict[str, Any]:
    """
    Return a compact, JSON-serialisable summary of the current state.

    Safe to emit to CloudWatch / Grafana at INFO level — strips full message
    content, raw options chain blobs, and full StrikeLeg lists.

    Attribute access uses only fields that exist on the canonical Pydantic
    contracts:
      TradeThesis   → .strategy_type.value, .confidence, .stop_loss_pct,
                       .target_profit_pct, .expected_entry_premium
      RiskAssessment→ .is_approved, .max_calculated_loss, .margin_required,
                       .tripped_risk_rules, .risk_engine_version
      ExecutionOrder→ .algo_id, .instrument.symbol, .quantity, .order_type.value

    Parameters
    ----------
    state : AgentState
        The current graph state dict.

    Returns
    -------
    dict[str, Any]
        Flat dict with key metrics.  All values are primitives (str, int,
        float, bool, None) — safe for JSON serialisation without a custom encoder.
    """
    thesis  = state.get("draft_thesis")
    risk    = state.get("risk_assessment")
    order   = state.get("execution_order")
    chain   = state.get("current_options_chain") or {}
    snap    = state.get("market_snapshot") or {}
    hist    = state.get("historical_context")

    return {
        # Session
        "session_id":              state.get("session_id"),
        "session_started_at":      state.get("session_started_at"),
        "evaluation_trigger":      state.get("evaluation_trigger"),
        "node_trace":              state.get("node_trace"),
        "message_count":           len(state.get("messages") or []),

        # Market
        "authorized_universe_size": len(state.get("authorized_universe") or []),
        "selected_underlying":      state.get("selected_underlying"),
        "selected_expiry":          state.get("selected_expiry"),
        "chain_atm_strike":         chain.get("atm_strike"),
        "chain_pcr_oi":             chain.get("pcr_oi"),
        "chain_max_pain":           chain.get("max_pain"),
        "india_vix":                snap.get("india_vix"),
        "market_regime":            snap.get("regime"),

        # Thesis — uses exact TradeThesis field names from contracts/research.py
        "thesis_id":               thesis.thesis_id if thesis else None,
        "thesis_strategy_type":    thesis.strategy_type.value if thesis else None,
        "thesis_confidence":       thesis.confidence if thesis else None,
        "thesis_stop_loss_pct":    thesis.stop_loss_pct if thesis else None,
        "thesis_target_pct":       thesis.target_profit_pct if thesis else None,
        "thesis_entry_premium":    float(thesis.expected_entry_premium) if thesis else None,
        "thesis_leg_count":        len(thesis.strike_legs) if thesis else None,
        "thesis_iteration":        state.get("thesis_iteration"),

        # Guardrails
        "event_risk_cleared":      state.get("event_risk_cleared"),
        "event_risk_event_name":   (state.get("event_risk_detail") or {}).get("event_name"),
        "historical_analogues":    len(hist) if hist is not None else None,
        "historical_win_rate":     state.get("historical_win_rate"),
        "historical_avg_pnl_inr":  state.get("historical_avg_pnl_inr"),

        # Risk — uses exact RiskAssessment field names from contracts/risk.py
        "risk_approved":           risk.is_approved if risk else None,
        "risk_approved_lots":      risk.approved_lots if risk else None,
        "risk_max_loss_inr":       float(risk.max_calculated_loss) if risk else None,
        "risk_margin_required":    float(risk.margin_required) if risk else None,
        "risk_tripped_rules":      risk.tripped_risk_rules if risk else None,
        "risk_engine_version":     risk.risk_engine_version if risk else None,

        # Execution — uses exact ExecutionOrder field names from contracts/execution.py
        "order_id":                order.order_id if order else None,
        "order_algo_id":           order.algo_id if order else None,
        "order_symbol":            order.instrument.symbol if order else None,
        "order_quantity":          order.quantity if order else None,
        "order_type":              order.order_type.value if order else None,
        "order_price":             float(order.price) if (order and order.price) else None,
        "order_is_complete":       order.is_complete if order else None,
        "order_exit_reason":       order.exit_reason.value if (order and order.exit_reason) else None,
        "order_realised_pnl_inr":  order.realised_pnl_inr if order else None,

        # Observability
        "abort_reason":            state.get("abort_reason"),
        "last_error":              state.get("last_error"),
    }


def is_ready_for_execution(state: AgentState) -> bool:
    """
    Hard gate: returns True only when ALL preconditions for execution are met.

    Called by the LangGraph conditional edge between RiskEngine and ExecutionNode.
    A single False from any condition blocks execution.

    Conditions (all must be True)
    ------------------------------
    1. event_risk_cleared is True
       Not None (gate not run), not False (event detected).
    2. historical_context is not None
       HistoricalContextGate ran.  Empty list [] is acceptable.
    3. risk_assessment is not None and risk_assessment.is_approved is True
       RiskEngine approved the thesis.
    4. execution_order is None
       Guards against double-execution on LangGraph checkpoint replay.
       If execution_order is already set, the graph has already sent orders.

    Parameters
    ----------
    state : AgentState
        The current graph state dict.

    Returns
    -------
    bool
    """
    risk = state.get("risk_assessment")
    return (
        state.get("event_risk_cleared") is True
        and state.get("historical_context") is not None
        and risk is not None
        and risk.is_approved is True
        and state.get("execution_order") is None
    )


def is_aborted(state: AgentState) -> bool:
    """
    Returns True if abort_reason is set — graph should route to END.

    Parameters
    ----------
    state : AgentState

    Returns
    -------
    bool
    """
    return state.get("abort_reason") is not None