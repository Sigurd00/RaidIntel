from __future__ import annotations
import time
from typing import Any, Dict, Iterable, List
from .models import Report, Fight, Event
from .wcl_client import WCLClient

GQL_REPORT_HEADER = """
query($code:String!){
  reportData {
    report(code:$code) {
      code
      title
      startTime
      endTime
    }
  }
}
"""

GQL_REPORTS = """
query($guildName:String!, $guildServerSlug:String!, $guildServerRegion:String!, $page:Int){
  reportData {
    reports(guildName:$guildName, guildServerSlug:$guildServerSlug, guildServerRegion:$guildServerRegion, page:$page){
      data { code title startTime endTime }
      has_more_pages
    }
  }
}
"""

GQL_FIGHTS = """
query($code:String!){
  reportData {
    report(code:$code) {
      fights { id startTime endTime }
    }
  }
}
"""

GQL_EVENTS = """
query($code:String!, $dataType:EventDataType!, $startTime:Float!, $endTime:Float!, $fightIDs:[Int]!, $limit:Int){
  reportData {
    report(code:$code) {
      events(
        dataType:$dataType
        startTime:$startTime
        endTime:$endTime
        fightIDs:$fightIDs
        limit:$limit
      ){
        nextPageTimestamp
        data
      }
    }
  }
}
"""

class WCLRepository:
    def __init__(self, client: WCLClient) -> None:
        self.client = client

    def get_report_header(self, code: str) -> Report:
        data = self.client.gql(GQL_REPORT_HEADER, {"code": code})
        rep = data["reportData"]["report"]
        if not rep:
            raise RuntimeError(f"Report not found: {code}")
        return Report(**rep)

    def list_guild_reports(self, guild: str, slug: str, region: str) -> List[Report]:
        page, out = 1, []
        while True:
            data = self.client.gql(GQL_REPORTS, {
                "guildName": guild, "guildServerSlug": slug, "guildServerRegion": region, "page": page
            })
            block = data["reportData"]["reports"]
            out.extend([Report(**r) for r in block["data"]])
            if not block.get("has_more_pages", False):
                return out
            page += 1
            time.sleep(0.2)

    def get_fights(self, code: str) -> List[Fight]:
        data = self.client.gql(GQL_FIGHTS, {"code": code})
        fights = data["reportData"]["report"]["fights"] or []
        return [Fight(**f) for f in fights if f.get("endTime", 0) > f.get("startTime", 0)]

    def stream_events(self, code: str, fight_id: int, start: float, end: float, data_type: str, limit: int = 10000) -> Iterable[Event]:
        cur = start
        while True:
            data = self.client.gql(GQL_EVENTS, {
                "code": code,
                "dataType": data_type,
                "startTime": cur,
                "endTime": end,
                "fightIDs": [fight_id],
                "limit": limit,
            })
            block = data["reportData"]["report"]["events"]
            events = block.get("data", []) or []
            for ev in events:
                yield ev
            nxt = block.get("nextPageTimestamp")
            if not nxt:
                break
            cur = float(nxt)
            time.sleep(0.15)
