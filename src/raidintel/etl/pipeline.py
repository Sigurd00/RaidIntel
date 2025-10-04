from __future__ import annotations
import os, csv, json, pathlib, logging
from typing import Iterable, List
from ..models import Fight, Event
from dataclasses import asdict

log = logging.getLogger(__name__)

class ETLPipeline:
    def __init__(self, output_dir: str = "out") -> None:
        self.output_dir = output_dir

    def _ensure_dir(self, path: str) -> None:
        pathlib.Path(path).mkdir(parents=True, exist_ok=True)

    def write_report_header_json(self, report: "Report") -> str:
        out_dir = os.path.join(self.output_dir, report.code)
        self._ensure_dir(out_dir)
        path = os.path.join(out_dir, "report.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(asdict(report), f, ensure_ascii=False, indent=2)
        return path
    
    def write_fights_csv(self, report_code: str, fights: List[Fight]) -> str:
        out_dir = os.path.join(self.output_dir, report_code)
        self._ensure_dir(out_dir)
        path = os.path.join(out_dir, "fights.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["id","startTime","endTime"])
            w.writeheader()
            for x in fights:
                w.writerow({"id": x.id, "startTime": x.startTime, "endTime": x.endTime})
        return path

    def dump_events_jsonl(self, report_code: str, fight: Fight, data_type: str, events: Iterable[Event]) -> str:
        out_dir = os.path.join(self.output_dir, report_code, "events")
        self._ensure_dir(out_dir)
        path = os.path.join(out_dir, f"fight_{fight.id}_{data_type}.jsonl")
        count = 0
        with open(path, "w", encoding="utf-8") as fh:
            for ev in events:
                fh.write(json.dumps(ev, ensure_ascii=False) + "\n")
                count += 1
        log.info("Wrote %s events to %s", count, path)
        return path
