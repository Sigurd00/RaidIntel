from __future__ import annotations
import typer
from .config import WCLConfig
from .wcl_client import WCLClient
from .repository import WCLRepository
from .etl.pipeline import ETLPipeline
from .storage import ArtifactStore
from .orchestrator import Context, Orchestrator
from .actions.core import AnalyzeGuildLatest, JustGetGuildData, EnsureReportEventsDumped, EnsureReportHeader, EnsureReportInGuild
from .actions.prescribe import BuildPlayerFeatures, PrescribeImprovements
from .graph import build_graph, render_ascii

app = typer.Typer(add_completion=False, help="RaidIntel CLI")

def _ctx(cfg_path: str) -> Context:
    cfg_path_lower = cfg_path.lower()
    if cfg_path_lower.endswith(".json"):
        cfg = WCLConfig.from_json(cfg_path)
    else:
        cfg = WCLConfig.from_toml(cfg_path)

    cfg.validate()
    client = WCLClient(site=cfg.site, client_id=cfg.client_id, client_secret=cfg.client_secret)
    repo = WCLRepository(client)
    etl = ETLPipeline(cfg.output_dir)
    store = ArtifactStore(cfg.output_dir)
    return Context(store=store, repo=repo, etl=etl, cfg=cfg)

@app.command()
def ensure_report_header(code: str, config: str = typer.Option("examples/raidintel.toml")):
    c = _ctx(config); Orchestrator(c).ensure(EnsureReportHeader(code))
    typer.echo(f"Ensured header for {code}")

@app.command()
def ensure_report_in_guild(code: str, config: str = typer.Option("examples/raidintel.toml")):
    c = _ctx(config); o = Orchestrator(c)
    o.ensure(EnsureReportInGuild(c.cfg.guild, c.cfg.server_slug, c.cfg.server_region, code))
    typer.echo(f"Ensured report {code} is in guild {c.cfg.guild}")

@app.command()
def list_reports(config: str = typer.Option("examples/raidintel.toml")):
    c = _ctx(config)
    reps = c.repo.list_guild_reports(c.cfg.guild, c.cfg.server_slug, c.cfg.server_region)
    typer.echo(f"Found {len(reps)} reports for {c.cfg.guild} ({c.cfg.server_slug}, {c.cfg.server_region.upper()})")
    for r in reps:
        typer.echo(f"{r.code}\t{r.title}\thttps://{c.cfg.site}.warcraftlogs.com/reports/{r.code}")

@app.command()
def dump_last_report(config: str = typer.Option("examples/raidintel.toml")):
    c = _ctx(config)
    reps = c.repo.list_guild_reports(c.cfg.guild, c.cfg.server_slug, c.cfg.server_region)
    if not reps: raise typer.Exit(code=1)
    latest = max(reps, key=lambda r: r.startTime)
    fights = c.repo.get_fights(latest.code)
    ETLPipeline(c.cfg.output_dir).write_fights_csv(latest.code, fights)
    for ft in fights:
        for et in c.cfg.event_types:
            ev = c.repo.stream_events(latest.code, ft.id, float(ft.startTime), float(ft.endTime), et)
            ETLPipeline(c.cfg.output_dir).dump_events_jsonl(latest.code, ft, et, ev)
    typer.echo(f"Dumped events for {latest.code}")

@app.command()
def dump_report(code: str, config: str = typer.Option("examples/raidintel.toml")):
    c = _ctx(config)
    orch = Orchestrator(c)
    orch.ensure(EnsureReportEventsDumped(code, c.cfg.event_types))

@app.command()
def run(action: str, config: str = typer.Option("examples/raidintel.toml")):
    c = _ctx(config); orch = Orchestrator(c)
    if action == "analyze-guild-latest":
        orch.ensure(AnalyzeGuildLatest(c.cfg.guild, c.cfg.server_slug, c.cfg.server_region))
    elif action == "just-get-guild-data":
        orch.ensure(JustGetGuildData(c.cfg.guild, c.cfg.server_slug, c.cfg.server_region))
    else:
        raise typer.BadParameter("Unknown action")

@app.command("print-graph")
def print_graph(action: str, config: str = typer.Option("examples/raidintel.toml")):
    c = _ctx(config)
    if action == "analyze-guild-latest":
        root = AnalyzeGuildLatest(c.cfg.guild, c.cfg.server_slug, c.cfg.server_region)
    elif action == "just-get-guild-data":
        root = JustGetGuildData(c.cfg.guild, c.cfg.server_slug, c.cfg.server_region)
    else:
        raise typer.BadParameter("Unknown action")
    nodes, edges, rid = build_graph(root, c)
    print(render_ascii(nodes, edges, rid))

@app.command()
def build_player_features_cmd(code: str, config: str = typer.Option("examples/raidintel.toml")):
    c = _ctx(config); Orchestrator(c).ensure(BuildPlayerFeatures(code)); print("player_features.csv written")

@app.command()
def prescribe(code: str, config: str = typer.Option("examples/raidintel.toml")):
    c = _ctx(config); Orchestrator(c).ensure(PrescribeImprovements(code)); print("coaching.md written")

if __name__ == "__main__":
    app()