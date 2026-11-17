"""Experiment readout and the per-segment EV table built from it."""
import math


def ev(p_action: float, p_none: float, margin: float, cost: float, unsub_term: float) -> float:
    """EV(a) = P(convert | a) * (margin - cost) - P(convert | none) * margin - unsubscribe term."""
    return p_action * (margin - cost) - p_none * margin - unsub_term


def experiment_rows(db, cfg) -> list:
    """One row per segment and arm, over users who passed the guardrails."""
    rows = db.execute(
        """
        SELECT json_extract(d.context, '$.segment') AS segment, d.arm,
               COUNT(*) AS n,
               SUM(EXISTS (SELECT 1 FROM events e WHERE e.user_id = d.user_id
                           AND e.type = 'recharge_success'
                           AND e.ts > d.decided_at AND e.ts <= d.decided_at + ?)) AS conversions,
               SUM(EXISTS (SELECT 1 FROM events e WHERE e.user_id = d.user_id
                           AND e.type = 'push_opt_out' AND e.ts > d.decided_at)) AS unsubscribes,
               SUM(d.send_status = 'sent') AS sent
        FROM decisions d WHERE d.reason = 'assigned'
        GROUP BY segment, d.arm ORDER BY segment, d.arm
        """,
        (cfg.conversion_window,),
    ).fetchall()
    out = []
    control = {r["segment"]: r for r in rows if r["arm"] == "control"}
    for r in rows:
        margin, bonus_cost = cfg.economics[r["segment"]]
        rate = r["conversions"] / r["n"]
        unsub = r["unsubscribes"] / r["n"]
        row = {"segment": r["segment"], "arm": r["arm"], "n": r["n"], "sent": r["sent"],
               "conversions": r["conversions"], "rate": rate, "unsub_rate": unsub}
        base = control.get(r["segment"])
        if base is not None and r["arm"] != "control":
            p0 = base["conversions"] / base["n"]
            se = math.sqrt(rate * (1 - rate) / r["n"] + p0 * (1 - p0) / base["n"])
            row["lift"] = rate - p0
            row["ci95"] = (rate - p0 - 1.96 * se, rate - p0 + 1.96 * se)
            cost = bonus_cost if r["arm"] == "bonus" else 0.0
            extra_unsub = max(unsub - base["unsubscribes"] / base["n"], 0.0)
            row["ev"] = ev(rate, p0, margin, cost, cfg.unsubscribe_penalty * extra_unsub)
        out.append(row)
    return out


def ev_table(rows: list) -> dict:
    """segment -> best action. 'none' wins unless some action has a positive EV."""
    best = {}
    for r in rows:
        if "ev" not in r:
            best.setdefault(r["segment"], ("none", 0.0))
        elif r["ev"] > best.get(r["segment"], ("none", 0.0))[1]:
            best[r["segment"]] = (r["arm"], r["ev"])
    return best


def funnel(db) -> list:
    return [(r["reason"], r["send_status"], r["note"], r["n"]) for r in db.execute(
        "SELECT reason, send_status, COALESCE(send_note, '') AS note, COUNT(*) AS n "
        "FROM decisions GROUP BY reason, send_status, note ORDER BY n DESC"
    )]
