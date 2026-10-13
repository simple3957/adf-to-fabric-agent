"""ADF trigger -> Fabric item schedule definitions.

Fabric runs a pipeline on a schedule through the Job Scheduler (``POST /v1/workspaces/{ws}/items/{id}/jobs/Pipeline/
schedules``, "Create Item Schedule"; verified 2026-10-13). A schedule is one of four documented configurations:

* ``Cron``    - ``interval`` minutes (1..5270400)
* ``Daily``   - ``times`` ["HH:mm", ...] (at most 100)
* ``Weekly``  - ``weekdays`` + ``times``
* ``Monthly`` - ``recurrence`` (1..12 months) + ``occurrence`` (DayOfMonth or OrdinalWeekday) + ``times``

each with ``startDateTime``, ``endDateTime`` and ``localTimeZoneId`` (a Windows time zone name, which is also what ADF
trigger ``timeZone`` uses). An item may have at most 20 schedules.

This module only produces definitions, and only where the ADF recurrence maps EXACTLY. When it does not (for example a
weekly trigger every second week, a "last day of the month" trigger, or more than 100 times a day), the result is
``manual`` with the reason; nothing is approximated silently. Tumbling-window triggers become interval (Cron) schedules
with a documented difference, because windowing semantics do not exist in Fabric. Event triggers are not generated: no
schedule API covers Activator rules.

Statuses: ``generated`` | ``generated_with_differences`` | ``manual`` | ``not_supported``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from adf2fabric.project.discovery import AdfRecurrence, AdfTrigger

MAX_SCHEDULES_PER_ITEM = 20
MAX_TIMES = 100
_WEEK_INDEX = {1: "First", 2: "Second", 3: "Third", 4: "Fourth", 5: "Fifth"}
_DAYS = {"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"}


@dataclass
class TriggerSchedule:
    status: str
    schedules: list[dict] = field(default_factory=list)  # Create Item Schedule bodies: {enabled, configuration}
    notes: list[str] = field(default_factory=list)


def _parse(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.strip().replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _stamp(dt: Optional[datetime]) -> Optional[str]:
    return dt.strftime("%Y-%m-%dT%H:%M:%S") if dt else None


def _slots(hours: list[int], minutes: list[int], start: Optional[datetime]) -> tuple[Optional[list[str]], bool]:
    """('HH:mm' slots, whether a default was taken from the start time). None when it cannot be determined."""
    defaulted = False
    if not hours:
        if start is None:
            return None, False
        hours, defaulted = [start.hour], True
    if not minutes:
        if start is None:
            return None, False
        minutes, defaulted = [start.minute], True
    if any(not 0 <= h <= 23 for h in hours) or any(not 0 <= m <= 59 for m in minutes):
        return None, False
    return sorted({f"{h:02d}:{m:02d}" for h in hours for m in minutes}), defaulted


def _config(kind: str, rec: AdfRecurrence, start: Optional[datetime], end: Optional[datetime], **extra) -> dict:
    cfg: dict = {"type": kind, **extra}
    if start:
        cfg["startDateTime"] = _stamp(start)
    if end:
        cfg["endDateTime"] = _stamp(end)
    cfg["localTimeZoneId"] = rec.time_zone or "UTC"
    return cfg


def build_schedules(t: AdfTrigger) -> TriggerSchedule:
    """Fabric schedule definitions for one ADF trigger (see the module docstring for the rules)."""
    kind = (t.type or "").lower()
    enabled = (t.runtime_state or "").lower() == "started"
    if "event" in kind or kind in ("blobeventstrigger", "customeventstrigger", "storageeventtrigger"):
        return TriggerSchedule("not_supported", notes=[
            "Event triggers map to Fabric Activator rules, which no schedule API creates; recreate the rule in Fabric."])
    if kind not in ("scheduletrigger", "tumblingwindowtrigger"):
        return TriggerSchedule("manual", notes=[f"Trigger type '{t.type}' has no documented Fabric schedule equivalent."])
    rec = t.recurrence
    if rec is None or not rec.frequency or not rec.interval:
        return TriggerSchedule("manual", notes=["The trigger's recurrence (frequency/interval) is missing or unreadable."])

    freq, n = rec.frequency.strip().lower(), rec.interval
    start, end = _parse(rec.start_time), _parse(rec.end_time)
    notes: list[str] = []
    differences = False
    if rec.start_time and start is None:
        return TriggerSchedule("manual", notes=[f"Could not read the start time '{rec.start_time}'."])
    if (rec.time_zone or "UTC").lower() != "utc":
        notes.append(f"Time zone '{rec.time_zone}': ADF and Fabric both use Windows time zone names; confirm the first "
                     "run time after creating the schedule.")
    if start and start < datetime.now():
        notes.append("The start time is in the past: Fabric triggers a run immediately when such a schedule is active.")
    tumbling = kind == "tumblingwindowtrigger"
    if tumbling:
        differences = True
        notes.append("Tumbling-window semantics (window start/end values, backfill, dependencies, delay, retry) do not "
                     "exist in Fabric; this is a plain interval schedule. Pipelines reading @trigger() window outputs "
                     "need another source for those values.")

    def manual(reason: str) -> TriggerSchedule:
        return TriggerSchedule("manual", notes=[*notes, reason])

    def done(schedules: list[dict]) -> TriggerSchedule:
        if len(schedules) > MAX_SCHEDULES_PER_ITEM:
            return manual(f"It needs {len(schedules)} schedules; Fabric allows {MAX_SCHEDULES_PER_ITEM} per item.")
        return TriggerSchedule("generated_with_differences" if differences else "generated",
                               [{"enabled": enabled, "configuration": c} for c in schedules], notes)

    has_schedule = bool(rec.minutes or rec.hours or rec.week_days or rec.month_days or rec.monthly_occurrences)

    if freq == "minute":
        if has_schedule or not 1 <= n <= 5270400:
            return manual("A minute recurrence with a schedule block, or an interval outside 1..5270400, is not expressible.")
        return done([_config("Cron", rec, start, end, interval=n)])

    if freq == "hour":
        if not (rec.hours or rec.week_days or rec.month_days or rec.monthly_occurrences) and not rec.minutes:
            if not 1 <= n * 60 <= 5270400:
                return manual("The interval is outside Fabric's 1..5270400 minute range.")
            return done([_config("Cron", rec, start, end, interval=n * 60)])
        if n == 1 and rec.minutes and not (rec.hours or rec.week_days or rec.month_days or rec.monthly_occurrences):
            slots, _ = _slots(list(range(24)), rec.minutes, start)
            if slots is None or len(slots) > MAX_TIMES:
                return manual(f"Every hour at {len(rec.minutes)} minutes needs more than {MAX_TIMES} daily times.")
            return done([_config("Daily", rec, start, end, times=slots)])
        return manual("An hourly recurrence with 'hours', or an interval above 1 with 'minutes', is not exactly expressible.")

    if freq == "day":
        if rec.week_days or rec.month_days or rec.monthly_occurrences:
            return manual("A daily recurrence with weekDays/monthDays is not valid in ADF or not expressible.")
        if not (rec.hours or rec.minutes):
            if n == 1:
                slots, _ = _slots([], [], start)
                if slots is None:
                    return manual("A daily trigger without a start time has no time of day to schedule.")
                return done([_config("Daily", rec, start, end, times=slots)])
            if not 1 <= n * 1440 <= 5270400:
                return manual("The interval is outside Fabric's range.")
            return done([_config("Cron", rec, start, end, interval=n * 1440)])
        if n != 1:
            return manual("Daily times on an interval above 1 day have no Fabric equivalent (Daily has no interval).")
        slots, defaulted = _slots(rec.hours, rec.minutes, start)
        if slots is None or len(slots) > MAX_TIMES:
            return manual("The hours/minutes cannot be turned into at most 100 valid times.")
        if defaulted:
            differences = True
            notes.append("ADF takes an omitted hours/minutes from the start time; the same default was applied here - confirm.")
        return done([_config("Daily", rec, start, end, times=slots)])

    if freq == "week":
        if n != 1:
            return manual("Weekly has no interval in Fabric, so every N weeks (N > 1) cannot be expressed.")
        days = [d for d in rec.week_days if d.lower() in _DAYS] or ([start.strftime("%A")] if start else [])
        if not days or len(days) != len(rec.week_days or days):
            return manual("The weekdays are missing or contain an unknown day.")
        slots, defaulted = _slots(rec.hours, rec.minutes, start)
        if slots is None or len(slots) > MAX_TIMES:
            return manual("The hours/minutes cannot be turned into at most 100 valid times.")
        if defaulted:
            differences = True
            notes.append("An omitted hours/minutes/weekDays was taken from the start time, as ADF does - confirm.")
        return done([_config("Weekly", rec, start, end, weekdays=[d.capitalize() for d in days], times=slots)])

    if freq == "month":
        if not 1 <= n <= 12:
            return manual("Monthly recurrence in Fabric is 1..12 months.")
        slots, defaulted = _slots(rec.hours, rec.minutes, start)
        if slots is None or len(slots) > MAX_TIMES:
            return manual("The hours/minutes cannot be turned into at most 100 valid times.")
        if defaulted:
            differences = True
            notes.append("An omitted hours/minutes was taken from the start time, as ADF does - confirm.")
        out: list[dict] = []
        if rec.monthly_occurrences:
            for o in rec.monthly_occurrences:
                day, occ = str(o.get("day") or ""), o.get("occurrence")
                if day.lower() not in _DAYS or occ not in _WEEK_INDEX:
                    return manual("A 'last' (-1) or invalid monthly occurrence has no Fabric equivalent (First..Fifth only).")
                out.append(_config("Monthly", rec, start, end, recurrence=n, times=slots, occurrence={
                    "occurrenceType": "OrdinalWeekday", "weekIndex": _WEEK_INDEX[occ], "weekday": day.capitalize()}))
        else:
            days = rec.month_days or ([start.day] if start else [])
            if not days or any(d < 1 or d > 31 for d in days):
                return manual("Negative (from the end of the month) or missing monthDays have no Fabric equivalent.")
            for d in sorted(set(days)):
                out.append(_config("Monthly", rec, start, end, recurrence=n, times=slots, occurrence={
                    "occurrenceType": "DayOfMonth", "dayOfMonth": d}))
        if len(out) > 1:
            notes.append(f"Fabric's Monthly takes one day per schedule, so {len(out)} schedules were generated.")
        return done(out)

    return manual(f"Frequency '{rec.frequency}' is not supported.")
