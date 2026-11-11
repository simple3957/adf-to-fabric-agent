"""ADF trigger -> Fabric schedule definitions: mapping rules, the files the agent writes, and creating them in Fabric."""

from __future__ import annotations

import json
from datetime import datetime

import pytest

from adf2fabric.agent.conversion import ConversionAgent, ConversionRequest
from adf2fabric.agent.conversion.report import render_report
from adf2fabric.agent.conversion.triggers import build_schedules
from adf2fabric.agent.discovery.adf_semantic import AdfSemanticDiscoverer, AdfSemanticDiscoveryRequest, parser
from adf2fabric.project.discovery import AdfRecurrence, AdfTrigger
from tests.test_adf_semantic_discovery import _make_project
from tests.test_conversion_agent import FILES
from tests.test_fabric_deploy import WS

FUTURE = "2099-01-01T08:30:00Z"


def trig(freq, interval=1, *, type_="ScheduleTrigger", state="Started", start=FUTURE, tz="UTC", end=None, **sched):
    rec = AdfRecurrence(frequency=freq, interval=interval, start_time=start, end_time=end, time_zone=tz, **sched)
    return AdfTrigger(name="TR", path="trigger/TR.json", type=type_, runtime_state=state, pipeline_refs=["PL"],
                      recurrence=rec)


def cfg(t):
    r = build_schedules(t)
    assert r.status in ("generated", "generated_with_differences"), (r.status, r.notes)
    return [s["configuration"] for s in r.schedules]


# --- mapping rules (each maps EXACTLY or is reported as manual) ---------------------------------------------


def test_minute_and_hour_intervals_become_cron_minutes():
    assert cfg(trig("Minute", 15))[0]["type"] == "Cron" and cfg(trig("Minute", 15))[0]["interval"] == 15
    assert cfg(trig("Hour", 2))[0]["interval"] == 120
    c = cfg(trig("Minute", 5, end="2099-02-01T00:00:00Z"))[0]
    assert c["startDateTime"] == "2099-01-01T08:30:00" and c["endDateTime"] == "2099-02-01T00:00:00"
    assert c["localTimeZoneId"] == "UTC"


def test_daily_uses_the_start_time_or_explicit_hours_and_minutes():
    assert cfg(trig("Day"))[0] == {"type": "Daily", "times": ["08:30"], "startDateTime": "2099-01-01T08:30:00",
                                   "endDateTime": "2108-12-30T08:30:00", "localTimeZoneId": "UTC"}
    c = cfg(trig("Day", hours=[8, 20], minutes=[0, 30]))[0]
    assert c["times"] == ["08:00", "08:30", "20:00", "20:30"]
    assert cfg(trig("Day", 3))[0] == {"type": "Cron", "interval": 4320, "startDateTime": "2099-01-01T08:30:00",
                                      "endDateTime": "2108-12-30T08:30:00", "localTimeZoneId": "UTC"}
    assert build_schedules(trig("Day", 2, hours=[8])).status == "manual"  # Daily has no interval


def test_hourly_at_given_minutes_is_exact_only_for_every_hour():
    c = cfg(trig("Hour", 1, minutes=[0, 30]))[0]
    assert c["type"] == "Daily" and len(c["times"]) == 48 and c["times"][:2] == ["00:00", "00:30"]
    assert build_schedules(trig("Hour", 2, minutes=[0])).status == "manual"


def test_weekly_maps_weekdays_but_not_every_other_week():
    c = cfg(trig("Week", 1, week_days=["Monday", "Friday"], hours=[9], minutes=[15]))[0]
    assert c["type"] == "Weekly" and c["weekdays"] == ["Monday", "Friday"] and c["times"] == ["09:15"]
    assert build_schedules(trig("Week", 2, week_days=["Monday"])).status == "manual"


def test_monthly_days_become_one_schedule_each_and_ordinals_map_first_to_fifth():
    cs = cfg(trig("Month", 1, month_days=[1, 15]))
    assert [c["occurrence"] for c in cs] == [{"occurrenceType": "DayOfMonth", "dayOfMonth": 1},
                                              {"occurrenceType": "DayOfMonth", "dayOfMonth": 15}]
    assert all(c["type"] == "Monthly" and c["recurrence"] == 1 and c["times"] == ["08:30"] for c in cs)
    o = cfg(trig("Month", 2, monthly_occurrences=[{"day": "Monday", "occurrence": 2}]))[0]
    assert o["occurrence"] == {"occurrenceType": "OrdinalWeekday", "weekIndex": "Second", "weekday": "Monday"}
    assert o["recurrence"] == 2


@pytest.mark.parametrize("t, why", [
    (trig("Month", 1, month_days=[-1]), "end of the month"),
    (trig("Month", 1, monthly_occurrences=[{"day": "Friday", "occurrence": -1}]), "last"),
    (trig("Month", 13), "1..12"),
    (trig("Day", 1, hours=list(range(24)), minutes=[0, 10, 20, 30, 40]), "100"),
    (trig("Month", 1, month_days=list(range(1, 22))), "20 per item"),
    (trig("Fortnight"), "not supported"),
    (trig("Day", start="not-a-date"), "start time"),
])
def test_what_fabric_cannot_express_exactly_is_manual_with_the_reason(t, why):
    r = build_schedules(t)
    assert r.status == "manual" and r.schedules == [] and any(why in n for n in r.notes), r.notes


def test_tumbling_window_is_an_interval_schedule_with_a_stated_difference():
    r = build_schedules(trig("Minute", 15, type_="TumblingWindowTrigger"))
    assert r.status == "generated_with_differences" and r.schedules[0]["configuration"]["interval"] == 15
    assert any("Tumbling-window" in n and "@trigger()" in n for n in r.notes)
    assert build_schedules(trig("Month", 1, type_="TumblingWindowTrigger", month_days=[1])).status != "generated"


def test_event_and_unknown_triggers_are_not_generated():
    ev = build_schedules(trig("Day", type_="BlobEventsTrigger"))
    assert ev.status == "not_supported" and "Activator" in ev.notes[0] and ev.schedules == []
    assert build_schedules(trig("Day", type_="WeirdTrigger")).status == "manual"
    assert build_schedules(AdfTrigger(name="T", path="p", type="ScheduleTrigger")).status == "manual"


def test_enabled_follows_the_source_state_and_caveats_are_noted():
    assert build_schedules(trig("Day", state="Started")).schedules[0]["enabled"] is True
    assert build_schedules(trig("Day", state="Stopped")).schedules[0]["enabled"] is False
    past = build_schedules(trig("Day", start="2020-01-01T00:00:00Z"))
    assert any("immediately" in n for n in past.notes)
    tz = build_schedules(trig("Day", tz="Pacific Standard Time"))
    assert tz.schedules[0]["configuration"]["localTimeZoneId"] == "Pacific Standard Time"
    assert any("time zone" in n.lower() for n in tz.notes)


def test_an_omitted_hours_or_minutes_is_flagged_as_defaulted_from_the_start_time():
    r = build_schedules(trig("Day", hours=[7]))  # minutes omitted -> start's minute (30)
    assert r.schedules[0]["configuration"]["times"] == ["07:30"] and r.status == "generated_with_differences"


def test_weekday_default_comes_from_the_start_date():
    thursday = datetime(2099, 1, 1).strftime("%A")
    assert cfg(trig("Week", 1))[0]["weekdays"] == [thursday]


# --- parser -------------------------------------------------------------------------------------------------


def test_parser_reads_the_full_recurrence_block():
    t = parser.parse_trigger({"name": "T", "properties": {"type": "ScheduleTrigger", "runtimeState": "Started",
        "pipelines": [{"pipelineReference": {"referenceName": "PL", "type": "PipelineReference"}}],
        "typeProperties": {"recurrence": {"frequency": "Month", "interval": 1, "startTime": "2026-01-01T00:00:00Z",
                                          "endTime": "2027-01-01T00:00:00Z", "timeZone": "UTC",
                                          "schedule": {"minutes": [0], "hours": [6], "monthDays": [1, -1],
                                                       "weekDays": ["Monday"], "monthlyOccurrences": [
                                                           {"day": "Friday", "occurrence": -1}]}}}}}, "trigger/T.json")
    r = t.recurrence
    assert (r.end_time, r.minutes, r.hours, r.month_days, r.week_days) == (
        "2027-01-01T00:00:00Z", [0], [6], [1, -1], ["Monday"])
    assert r.monthly_occurrences == [{"day": "Friday", "occurrence": -1}]


# --- the agent writes definition files ----------------------------------------------------------------------


def _project(tmp_path, trigger):
    files = dict(FILES)
    files["trigger/TR_Nightly.json"] = trigger
    store, pid = _make_project(tmp_path, files)
    AdfSemanticDiscoverer(store).run(AdfSemanticDiscoveryRequest(project_name="test-proj"))
    return store, pid


def _trigger_json(freq="Day", interval=1, state="Started", **extra):
    return {"name": "TR_Nightly", "properties": {
        "type": "ScheduleTrigger", "runtimeState": state,
        "pipelines": [{"pipelineReference": {"referenceName": "PL_Main", "type": "PipelineReference"}}],
        "typeProperties": {"recurrence": {"frequency": freq, "interval": interval, "startTime": FUTURE,
                                          "timeZone": "UTC", **extra}}}}


def test_agent_writes_one_schedule_file_per_trigger_and_records_it(tmp_path):
    store, pid = _project(tmp_path, _trigger_json())
    r = ConversionAgent(store).run(ConversionRequest(project_name="test-proj"))
    assert r.success
    note = next(t for p in r.conversion.pipelines for t in p.triggers if t.name == "TR_Nightly")
    assert note.schedule_status == "generated_with_differences" and note.schedule_file == "schedules/TR_Nightly.json"
    body = json.loads((store.conversion_dir(pid) / note.schedule_file).read_text(encoding="utf-8"))
    assert body["jobType"] == "Pipeline" and body["pipelines"] == ["PL_Main"]
    assert body["schedules"] == [{"enabled": True, "configuration": {
        "type": "Daily", "times": ["08:30"], "startDateTime": "2099-01-01T08:30:00",
        "endDateTime": "2108-12-30T08:30:00", "localTimeZoneId": "UTC"}}]
    text = (store.conversion_dir(pid) / "conversion-report.md").read_text(encoding="utf-8")
    assert "Fabric schedule: **generated_with_differences**" in text and "schedules/TR_Nightly.json" in text


def test_a_trigger_that_cannot_map_writes_no_file_and_says_why(tmp_path):
    store, pid = _project(tmp_path, _trigger_json("Week", 2, schedule={"weekDays": ["Monday"]}))
    r = ConversionAgent(store).run(ConversionRequest(project_name="test-proj"))
    note = next(t for p in r.conversion.pipelines for t in p.triggers if t.name == "TR_Nightly")
    assert note.schedule_status == "manual" and note.schedule_file is None and not note.schedules
    assert any("every N weeks" in n for n in note.schedule_notes)
    assert not (store.conversion_dir(pid) / "schedules").exists()


def test_a_clean_estate_has_no_compatibility_section(tmp_path):
    store, pid = _project(tmp_path, _trigger_json())
    r = ConversionAgent(store).run(ConversionRequest(project_name="test-proj"))
    assert "## Compatibility findings" not in render_report(r.conversion)


# --- Fabric requires an end time (verified live 2026-10-16: "The EndDateTime field is required") --------------


def test_every_generated_schedule_has_an_end_time_even_when_the_trigger_has_none():
    r = build_schedules(trig("Day"))
    assert r.schedules[0]["configuration"]["endDateTime"] == "2108-12-30T08:30:00"
    assert r.status == "generated_with_differences"
    assert any("no end time" in n and "ten years" in n for n in r.notes)


def test_the_triggers_own_end_time_is_kept_and_is_not_a_difference():
    r = build_schedules(trig("Day", end="2099-06-01T00:00:00Z"))
    assert r.schedules[0]["configuration"]["endDateTime"] == "2099-06-01T00:00:00"
    assert r.status == "generated" and not any("no end time" in n for n in r.notes)


def test_a_start_in_the_past_still_gets_an_end_in_the_future():
    cfg_ = build_schedules(trig("Day", start="2020-01-01T00:00:00Z")).schedules[0]["configuration"]
    assert datetime.fromisoformat(cfg_["endDateTime"]) > datetime.now()


def test_an_unreadable_end_time_is_manual_not_guessed():
    r = build_schedules(trig("Day", end="not-a-date"))
    assert r.status == "manual" and any("end time" in n for n in r.notes)


# --- what a person needs to recreate by hand ---------------------------------------------------------------


def _event_json():
    return {"name": "TR_Nightly", "properties": {
        "type": "BlobEventsTrigger", "runtimeState": "Started",
        "pipelines": [{"pipelineReference": {"referenceName": "PL_Main", "type": "PipelineReference"},
                       "parameters": {"fileName": "@triggerBody().fileName", "folder": "@triggerBody().folderPath",
                                      "mode": "incremental", "retries": 3,
                                      "key": "AccountKey=abcdefghijklmnop;EndpointSuffix=core.windows.net"}}],
        "typeProperties": {"scope": "/subscriptions/s/resourceGroups/rg/providers/Microsoft.Storage/storageAccounts/acct",
                           "events": ["Microsoft.Storage.BlobCreated"], "blobPathBeginsWith": "/landing/blobs/in/",
                           "blobPathEndsWith": ".csv", "ignoreEmptyBlobs": True}}}


def test_parser_keeps_event_settings_and_the_parameters_a_trigger_passes():
    t = parser.parse_trigger(_event_json(), "trigger/T.json")
    assert t.event.scope.endswith("storageAccounts/acct") and t.event.events == ["Microsoft.Storage.BlobCreated"]
    assert (t.event.blob_path_begins_with, t.event.blob_path_ends_with, t.event.ignore_empty_blobs) == (
        "/landing/blobs/in/", ".csv", True)
    assert t.pipeline_parameters["PL_Main"] == {"fileName": "@triggerBody().fileName",
                                                "folder": "@triggerBody().folderPath", "mode": "incremental",
                                                "retries": "3"}  # the connection-string-shaped value is dropped


def test_a_schedule_trigger_has_no_event_block():
    assert parser.parse_trigger(_trigger_json(), "trigger/T.json").event is None


def test_report_lists_event_settings_and_flags_trigger_scope_parameters(tmp_path):
    store, pid = _project(tmp_path, _event_json())
    r = ConversionAgent(store).run(ConversionRequest(project_name="test-proj"))
    note = next(t for p in r.conversion.pipelines for t in p.triggers if t.name == "TR_Nightly")
    assert note.schedule_status == "not_supported" and not note.schedules
    text = " | ".join(note.details)
    assert "storageAccounts/acct" in text and "Microsoft.Storage.BlobCreated" in text and ".csv" in text
    assert "Parameter `mode` of `PL_Main` was set to `incremental`" in text
    assert "pipeline().TriggerTime" in next(d for d in note.details if "`fileName`" in d)
    assert "TriggerTime" not in next(d for d in note.details if "`mode`" in d)
    assert "AccountKey" not in text
    assert "Listens to:" in (store.conversion_dir(pid) / "conversion-report.md").read_text(encoding="utf-8")


def test_a_schedule_trigger_without_parameters_has_nothing_to_re_enter(tmp_path):
    store, pid = _project(tmp_path, _trigger_json())
    r = ConversionAgent(store).run(ConversionRequest(project_name="test-proj"))
    note = next(t for p in r.conversion.pipelines for t in p.triggers if t.name == "TR_Nightly")
    assert note.details == [] and "re-enter" not in note.action


# --- creating schedules in Fabric ---------------------------------------------------------------------------

LS = f"/workspaces/{WS}/items/ITEM1/jobs/Pipeline/schedules"


