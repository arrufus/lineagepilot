"""Generate a synthetic, realistically shaped dbt project.

Shape (design section 14 asks for a synthetic 5,000-model manifest):

- sources -> staging -> intermediate -> marts, about 30% / 45% / 25% of models;
- 10-30 columns per model, every column documented in YAML;
- generic tests in YAML: unique + not_null on each primary key, relationships
  on foreign keys, some accepted_values, some source tests;
- ref fan-in of 1-4, with skewed (Pareto) parent popularity so a few nodes
  become hubs, as in real projects;
- exposures on marts, meta.owner on most models (YAML and folder level);
- one versioned model (v1 and v2), one disabled model, two seeds, one
  snapshot, a few incremental models and a few that end in an explicit
  select list instead of ``select * from final``.

The SQL is valid DuckDB, so ``dbt run`` and ``dbt docs generate`` can build a
real catalog.json. Output is deterministic for a given --models and --seed.

    uv run python scripts/gen_project.py --models 2000 --out work/projects/synth-2000
"""

from __future__ import annotations

import argparse
import json
import random
import shutil
from dataclasses import dataclass, field
from pathlib import Path

import yaml

DOMAINS = ["finance", "sales", "marketing", "product", "operations", "support",
           "logistics", "billing", "growth", "risk", "inventory", "people"]
SYSTEMS = {"erp": "finance", "crm": "sales", "web": "marketing", "app": "product",
           "pos": "sales", "billing_sys": "billing", "hris": "people", "ads": "marketing",
           "desk": "support", "wms": "inventory", "payments": "finance", "fleet": "logistics"}
NOUNS = ["orders", "customers", "invoices", "payments", "products", "shipments", "tickets",
         "sessions", "events", "accounts", "contracts", "refunds", "leads", "campaigns",
         "employees", "suppliers", "warehouses", "devices", "subscriptions", "returns",
         "carts", "visits", "claims", "quotes", "transfers", "budgets", "routes", "stores"]
VERBS = ["joined", "enriched", "deduped", "pivoted", "unioned", "filtered", "scored", "rolled_up"]
TEAMS = ["analytics-eng", "finance-data", "growth-data", "platform-data", "ops-insights", "risk-analytics"]

ATTRS: dict[str, list[str]] = {
    "num": ["amount", "quantity", "price", "cost", "discount", "tax", "revenue", "score",
            "balance", "weight", "duration_sec", "unit_price", "total", "margin"],
    "str": ["status", "category", "channel", "region", "country", "city", "currency", "email",
            "first_name", "last_name", "phone", "segment", "plan", "sku", "tier", "device_type",
            "ip_address", "source_name"],
    "time": ["created_at", "updated_at", "order_date", "shipped_at", "closed_at", "signup_date",
             "event_ts", "due_date", "paid_at"],
    "bool": ["is_active", "is_deleted", "is_test", "has_discount", "is_paid", "is_returned"],
}
RAW_TYPE = {"num": ["decimal(18,4)", "integer", "double"], "str": ["varchar"],
            "time": ["timestamp", "date"], "bool": ["boolean"]}


@dataclass
class Col:
    name: str
    fam: str  # num | str | time | bool | key


@dataclass
class Model:
    name: str
    layer: str  # staging | intermediate | marts
    domain: str
    folder: str
    entity: str
    cols: list[Col] = field(default_factory=list)
    parents: list[str] = field(default_factory=list)  # model or seed names
    source: tuple[str, str] | None = None
    fks: list[tuple[str, str]] = field(default_factory=list)  # (column, parent model)
    weight: float = 1.0
    versioned: bool = False

    @property
    def pk(self) -> str:
        return f"{self.entity}_id"


class Gen:
    def __init__(self, n_models: int, seed: int, yaml_style: str) -> None:
        self.n = n_models
        self.r = random.Random(seed)
        self.yaml_style = yaml_style
        self.models: dict[str, Model] = {}
        self.order: list[str] = []
        self.sources: dict[tuple[str, str], list[tuple[str, str, str]]] = {}  # -> [(col, fam, sqltype)]
        self.noun_counter: dict[str, int] = {}
        self.sql: dict[str, str] = {}

    # ---------------------------------------------------------------- helpers
    def uniq(self, stem: str) -> str:
        k = self.noun_counter.get(stem, 0) + 1
        self.noun_counter[stem] = k
        return f"{stem}_{k}"

    def pick_parents(self, candidates: list[Model], k: int, domain: str,
                     layer_boost: dict[str, float] | None = None) -> list[Model]:
        chosen: list[Model] = []
        pool = list(candidates)
        boost = layer_boost or {}
        weights = [m.weight * (3.0 if m.domain == domain else 1.0) * boost.get(m.layer, 1.0) for m in pool]
        for _ in range(min(k, len(pool))):
            m = self.r.choices(pool, weights=weights, k=1)[0]
            i = pool.index(m)
            pool.pop(i)
            weights.pop(i)
            chosen.append(m)
        return chosen

    def fan_in(self) -> int:
        return self.r.choices([1, 2, 3, 4], weights=[30, 35, 20, 15])[0]

    def pareto(self) -> float:
        return self.r.paretovariate(1.2)

    @staticmethod
    def free(name: str, used: set[str], prefix: str) -> str:
        for cand in (name, f"{prefix}_{name}"):
            if cand not in used:
                used.add(cand)
                return cand
        k = 2
        while f"{name}_{k}" in used:
            k += 1
        used.add(f"{name}_{k}")
        return f"{name}_{k}"

    def transform(self, ref: str, fam: str) -> tuple[str, str]:
        """Return (sql expression, output family) for a column reference."""
        r = self.r.random()
        if fam == "num":
            opts = [(ref, "num"), (f"cast({ref} as decimal(18,2))", "num"), (f"{ref} * 1.1", "num"),
                    (f"coalesce({ref}, 0)", "num"),
                    (f"case when {ref} > 100 then 'high' when {ref} > 10 then 'medium' else 'low' end", "str")]
        elif fam == "str":
            opts = [(ref, "str"), (f"upper({ref})", "str"), (f"lower(trim({ref}))", "str"),
                    (f"coalesce({ref}, 'unknown')", "str"),
                    (f"case when {ref} is null then 'n/a' else {ref} end", "str")]
        elif fam == "time":
            opts = [(ref, "time"), (f"cast({ref} as date)", "time"), (f"date_trunc('month', {ref})", "time")]
        else:
            opts = [(ref, "bool"), (f"coalesce({ref}, false)", "bool")]
        return opts[0] if r < 0.45 else self.r.choice(opts[1:])

    # ---------------------------------------------------------------- layers
    def build(self) -> None:
        n_stg = round(self.n * 0.30)
        n_mart = round(self.n * 0.25)
        n_int = self.n - n_stg - n_mart - 1  # the versioned mart adds one extra node
        systems = list(SYSTEMS)
        for i in range(n_stg):
            system = systems[i % len(systems)]
            self.add_staging(system)
        for _ in range(n_int):
            self.add_intermediate()
        for j in range(n_mart):
            self.add_mart(versioned=(j == 3))

    def add_staging(self, system: str) -> None:
        table = self.uniq(self.r.choice(NOUNS))
        entity = table
        name = f"stg_{system}__{table}"
        n_cols = self.r.randint(10, 30)
        raw: list[tuple[str, str, str]] = [("id", "key", "integer")]
        cols = [Col(f"{entity}_id", "key")]
        exprs = [f"        id as {entity}_id"]
        used = {f"{entity}_id"}
        fks: list[tuple[str, str]] = []
        siblings = [m for m in self.models.values() if m.layer == "staging" and m.source and m.source[0] == system]
        if siblings and self.r.random() < 0.5:
            other = self.r.choice(siblings[-20:])
            raw.append((other.pk, "key", "integer"))
            cols.append(Col(other.pk, "key"))
            used.add(other.pk)
            exprs.append(f"        {other.pk}")
            fks.append((other.pk, other.name))
        while len(cols) < n_cols:
            fam = self.r.choice(["num", "num", "str", "str", "str", "time", "bool"])
            base = self.r.choice(ATTRS[fam])
            out = self.free(base, used, entity.split("_")[0])
            raw_name = f"src_{out}" if self.r.random() < 0.5 else out
            raw.append((raw_name, fam, self.r.choice(RAW_TYPE[fam])))
            if fam == "num":
                e = f"cast({raw_name} as decimal(18,2))"
            elif fam == "str":
                e = self.r.choice([raw_name, f"nullif(trim({raw_name}), '')", f"lower({raw_name})"])
            elif fam == "time":
                e = self.r.choice([f"cast({raw_name} as timestamp)", raw_name])
            else:
                e = raw_name
            exprs.append(f"        {e} as {out}" if e != out else f"        {out}")
            cols.append(Col(out, fam))
        raw.append(("_loaded_at", "time", "timestamp"))
        self.sources[(system, table)] = raw
        sql = (
            "with\n\nsource as (\n\n"
            f"    select * from {{{{ source('{system}', '{table}') }}}}\n\n),\n\n"
            "renamed as (\n\n    select\n" + ",\n".join(exprs) + "\n\n    from source\n\n)\n\n"
            "select * from renamed\n"
        )
        m = Model(name, "staging", SYSTEMS[system], f"staging/{system}", entity, cols,
                  source=(system, table), fks=fks, weight=self.pareto())
        self.register(m, sql)

    def joined_model_sql(self, m: Model, parents: list[Model], allow_agg: bool) -> str:
        """Common SQL shape for intermediate and mart models."""
        used: set[str] = set()
        aliases = {}
        ctes = []
        for p in parents:
            alias = p.entity if p.entity not in aliases.values() else p.entity + "_b"
            aliases[p.name] = alias
            ctes.append(f"{alias} as (\n\n    select * from {{{{ ref('{p.name}') }}}}\n\n)")
        a = parents[0]
        a_alias = aliases[a.name]
        select = [f"        {a_alias}.{a.pk} as {m.pk}"]
        used.add(m.pk)
        m.cols.append(Col(m.pk, "key"))
        joins = []
        sources_for_attrs: list[tuple[str, Col, str]] = []  # (alias, col, prefix)
        for c in a.cols[1:]:
            sources_for_attrs.append((a_alias, c, a.entity.split("_")[0]))
        for p in parents[1:]:
            alias = aliases[p.name]
            if allow_agg and self.r.random() < 0.4 and any(c.fam == "num" for c in p.cols):
                agg = f"{alias}_agg"
                nums = [c for c in p.cols if c.fam == "num"][:3]
                agg_cols = [f"        {p.pk}", "        count(*) as row_count"] + [
                    f"        sum({c.name}) as total_{c.name}" for c in nums]
                ctes.append(f"{agg} as (\n\n    select\n" + ",\n".join(agg_cols) +
                            f"\n    from {alias}\n    group by 1\n\n)")
                joins.append(f"    left join {agg} on {a_alias}.{a.pk} = {agg}.{p.pk}")
                fk = self.free(p.pk, used, "agg")
                select.append(f"        {agg}.{p.pk} as {fk}" if fk != p.pk else f"        {agg}.{p.pk}")
                m.cols.append(Col(fk, "key"))
                m.fks.append((fk, p.name))
                pre = p.entity.split("_")[0]
                for nm in ["row_count"] + [f"total_{c.name}" for c in nums]:
                    sources_for_attrs.append((agg, Col(nm, "num"), pre))
            else:
                joins.append(f"    left join {alias} on {a_alias}.{a.pk} = {alias}.{p.pk}")
                fk = self.free(p.pk, used, "ref")
                select.append(f"        {alias}.{p.pk} as {fk}" if fk != p.pk else f"        {alias}.{p.pk}")
                m.cols.append(Col(fk, "key"))
                m.fks.append((fk, p.name))
                for c in p.cols[1:]:
                    sources_for_attrs.append((alias, c, p.entity.split("_")[0]))
        target = self.r.randint(10, 30)
        self.r.shuffle(sources_for_attrs)
        temporal = []
        for alias, c, pre in sources_for_attrs:
            if len(m.cols) >= target:
                break
            if c.fam == "key":
                continue
            expr, fam = self.transform(f"{alias}.{c.name}", c.fam)
            out = self.free(c.name if fam == c.fam else f"{c.name}_band", used, pre)
            select.append(f"        {expr} as {out}")
            m.cols.append(Col(out, fam))
            if fam == "time" and expr == f"{alias}.{c.name}":
                temporal.append(f"{alias}.{c.name}")
        if len(temporal) >= 2 and len(m.cols) < 30:
            out = self.free("days_between", used, "calc")
            select.append(f"        datediff('day', {temporal[0]}, {temporal[1]}) as {out}")
            m.cols.append(Col(out, "num"))
        while len(m.cols) < 10:  # thin parents: pad with derived flags
            out = self.free("is_recent", used, "calc")
            select.append(f"        {a_alias}.{a.pk} > 100 as {out}")
            m.cols.append(Col(out, "bool"))
        body = (
            "joined as (\n\n    select\n" + ",\n".join(select) +
            f"\n\n    from {a_alias}\n" + "\n".join(joins) + ("\n" if joins else "") + "\n)"
        )
        ctes.append(body)
        header = ""
        roll = self.r.random()
        tcols = [c.name for c in m.cols if c.fam == "time"]
        if roll < 0.05 and tcols:
            header = f"{{{{ config(materialized='incremental', unique_key='{m.pk}') }}}}\n\n"
            tail = (f"select * from joined\n\n{{% if is_incremental() %}}\n"
                    f"where {tcols[0]} > (select max({tcols[0]}) from {{{{ this }}}})\n{{% endif %}}\n")
        elif roll < 0.25:
            tail = "select\n" + ",\n".join(f"    {c.name}" for c in m.cols) + f"\nfrom joined\nwhere {m.pk} is not null\n"
        else:
            tail = "select * from joined\n"
        return header + "with\n\n" + ",\n\n".join(ctes) + "\n\n" + tail

    def add_intermediate(self) -> None:
        domain = self.r.choice(DOMAINS)
        noun = self.r.choice(NOUNS)
        entity = self.uniq(f"{domain}_{noun}")
        name = f"int_{domain}__{noun}_{self.r.choice(VERBS)}_{entity.rsplit('_', 1)[1]}"
        cands = [m for m in self.models.values() if m.layer in ("staging", "intermediate")]
        parents = self.pick_parents(cands, self.fan_in(), domain)
        m = Model(name, "intermediate", domain, f"intermediate/{domain}", entity,
                  parents=[p.name for p in parents], weight=self.pareto())
        sql = self.joined_model_sql(m, parents, allow_agg=False)
        self.register(m, sql)

    def add_mart(self, versioned: bool) -> None:
        domain = self.r.choice(DOMAINS)
        noun = self.r.choice(NOUNS)
        kind = "dim" if self.r.random() < 0.4 else "fct"
        entity = self.uniq(f"{kind}_{noun}")
        name = f"{kind}_{domain}__{entity}"
        parents = self.pick_parents(list(self.models.values()), self.fan_in(), domain,
                                    layer_boost={"intermediate": 3.0, "staging": 1.0, "marts": 1.0})
        m = Model(name, "marts", domain, f"marts/{domain}", entity,
                  parents=[p.name for p in parents], weight=self.pareto(), versioned=versioned)
        sql = self.joined_model_sql(m, parents, allow_agg=True)
        self.register(m, sql)

    def register(self, m: Model, sql: str) -> None:
        self.models[m.name] = m
        self.order.append(m.name)
        self.sql[m.name] = sql

    # ---------------------------------------------------------------- writers
    def test_entry(self, test: str, args: dict | None):
        if not args:
            return test
        if self.yaml_style == "v2":
            return {test: {"arguments": args}}
        return {test: args}

    def column_yaml(self, m: Model, c: Col) -> dict:
        d: dict = {"name": c.name, "description": self.describe_col(m, c)}
        tests = []
        if c.name == m.pk:
            tests += ["unique", "not_null"]
        for fk, parent in m.fks:
            if fk == c.name:
                p = self.models[parent]
                tests.append(self.test_entry("relationships", {"to": f"ref('{p.name}')", "field": p.pk}))
                if self.r.random() < 0.5:
                    tests.append("not_null")
        if c.name == "status" and self.r.random() < 0.5:
            tests.append(self.test_entry("accepted_values", {"values": ["active", "inactive", "pending"]}))
        if tests:
            d["data_tests"] = tests
        return d

    def describe_col(self, m: Model, c: Col) -> str:
        if c.name == m.pk:
            return f"Primary key of {m.name}. One row per {m.entity.replace('_', ' ')}."
        if c.fam == "key":
            return f"Foreign key to {c.name[:-3].replace('_', ' ')}. Used to join to its parent model."
        return (f"The {c.name.replace('_', ' ')} of each {m.entity.split('_')[0]} record, "
                f"carried from upstream and standardised in the {m.layer} layer.")

    def write(self, out: Path) -> dict:
        if out.exists():
            shutil.rmtree(out)
        (out / "models").mkdir(parents=True)
        by_folder: dict[str, list[Model]] = {}
        for name in self.order:
            by_folder.setdefault(self.models[name].folder, []).append(self.models[name])
        exposures: dict[str, list[dict]] = {}
        marts = [self.models[n] for n in self.order if self.models[n].layer == "marts"]
        for i in range(0, len(marts), 25):
            group = marts[i:i + 25]
            deps = self.r.sample(group, k=min(len(group), self.r.randint(1, 6)))
            dom = deps[0].domain
            exposures.setdefault(f"marts/{dom}", []).append({
                "name": f"{dom}_dashboard_{i // 25 + 1}",
                "type": self.r.choice(["dashboard", "application", "analysis", "notebook"]),
                "maturity": self.r.choice(["high", "medium", "low"]),
                "owner": {"name": f"{dom.title()} Team", "email": f"{dom}-team@example.com"},
                "description": f"{dom.title()} reporting surface built on {len(deps)} marts.",
                "depends_on": [f"ref('{d.name}')" for d in deps],
            })
        n_tests = 0
        for folder, models in by_folder.items():
            d = out / "models" / folder
            d.mkdir(parents=True, exist_ok=True)
            entries = []
            for m in models:
                if m.versioned:
                    (d / f"{m.name}_v1.sql").write_text(self.sql[m.name])
                    (d / f"{m.name}_v2.sql").write_text(self.sql[m.name])
                else:
                    (d / f"{m.name}.sql").write_text(self.sql[m.name])
                e: dict = {"name": m.name,
                           "description": f"{m.layer.title()} model for {m.entity.replace('_', ' ')} in the "
                                          f"{m.domain} domain. Built from {len(m.parents) or 1} upstream node(s)."}
                cfg: dict = {}
                if m.layer != "staging" and self.r.random() < 0.7:
                    cfg["meta"] = {"owner": self.r.choice(TEAMS), "domain": m.domain}
                if self.r.random() < 0.3:
                    cfg["tags"] = [m.layer, m.domain]
                if cfg:
                    e["config"] = cfg
                e["columns"] = [self.column_yaml(m, c) for c in m.cols]
                n_tests += sum(len(c.get("data_tests", [])) for c in e["columns"])
                if m.versioned:
                    e["latest_version"] = 2
                    e["versions"] = [{"v": 1}, {"v": 2}]
                entries.append(e)
            doc = {"version": 2, "models": entries}
            if folder in exposures:
                doc["exposures"] = exposures[folder]
            (d / f"_{folder.split('/')[-1]}__models.yml").write_text(yaml.safe_dump(doc, sort_keys=False, width=120))
        # sources, one file per system
        for system in SYSTEMS:
            tables = [(t, cols) for (s, t), cols in self.sources.items() if s == system]
            if not tables:
                continue
            tdocs = []
            for t, cols in tables:
                td: dict = {"name": t, "description": f"Raw {t.replace('_', ' ')} from {system}."}
                if self.r.random() < 0.5:
                    td["columns"] = [{"name": "id", "description": "Source primary key.",
                                      "data_tests": ["unique", "not_null"]}] + [
                        {"name": c, "description": f"Raw {c} as loaded."} for c, _, _ in cols[1:]]
                    n_tests += 2
                tdocs.append(td)
            src: dict = {"name": system, "schema": f"raw_{system}",
                         "description": f"Raw tables replicated from {system}.", "loader": "fivetran"}
            fresh = {"loaded_at_field": "_loaded_at", "freshness": {"warn_after": {"count": 12, "period": "hour"}}}
            if self.yaml_style == "v2":
                src["config"] = fresh  # dbt v2 rejects these as top-level source keys
            else:
                src.update(fresh)
            src["tables"] = tdocs
            sdoc = {"version": 2, "sources": [src]}
            (out / "models" / "staging" / system / f"_{system}__sources.yml").write_text(
                yaml.safe_dump(sdoc, sort_keys=False, width=120))
        # disabled model, seeds, snapshot
        (out / "models" / "intermediate" / "finance").mkdir(parents=True, exist_ok=True)
        (out / "models" / "intermediate" / "finance" / "int_finance__legacy_disabled.sql").write_text(
            "{{ config(enabled=false) }}\nselect 1 as id\n")
        (out / "seeds").mkdir()
        (out / "seeds" / "country_codes.csv").write_text(
            "country_id,country_code,country_name\n1,GB,United Kingdom\n2,US,United States\n3,NG,Nigeria\n")
        (out / "seeds" / "currency_rates.csv").write_text(
            "currency_id,currency_code,rate_to_usd\n1,GBP,1.27\n2,USD,1.0\n3,EUR,1.08\n")
        (out / "snapshots").mkdir()
        snap_src = next(self.models[n] for n in self.order if self.models[n].layer == "staging")
        (out / "snapshots" / "snap_first_staging.sql").write_text(
            "{% snapshot snap_first_staging %}\n"
            f"{{{{ config(target_schema='snapshots', unique_key='{snap_src.pk}', strategy='check', check_cols='all') }}}}\n"
            f"select * from {{{{ ref('{snap_src.name}') }}}}\n{{% endsnapshot %}}\n")
        # a couple of models read seeds, so seeds get edges too
        first_int = next(self.models[n] for n in self.order if self.models[n].layer == "intermediate")
        (out / "models" / "intermediate" / first_int.domain / "int_ref_seed_countries.sql").write_text(
            f"select c.country_id, c.country_code, c.country_name, m.{first_int.pk}\n"
            f"from {{{{ ref('country_codes') }}}} as c\n"
            f"left join {{{{ ref('{first_int.name}') }}}} as m on c.country_id = m.{first_int.pk}\n")
        project = {
            "name": "synthetic", "version": "1.0.0", "config-version": 2, "profile": "synthetic",
            "model-paths": ["models"], "seed-paths": ["seeds"], "snapshot-paths": ["snapshots"],
            "target-path": "target",
            "models": {"synthetic": {
                "+materialized": "view",
                "marts": {"finance": {"+meta": {"owner": "finance-data"}},
                          "risk": {"+meta": {"owner": "risk-analytics"}}},
            }},
        }
        (out / "dbt_project.yml").write_text(yaml.safe_dump(project, sort_keys=False))
        ddl = []
        for system in SYSTEMS:
            ddl.append(f"create schema if not exists raw_{system};")
        for (system, table), cols in self.sources.items():
            ddl.append(f"create table if not exists raw_{system}.{table} (" +
                       ", ".join(f"{c} {t}" for c, _, t in cols) + ");")
        (out / "raw_tables.sql").write_text("\n".join(ddl) + "\n")
        n_models = len(self.order) + 1  # +1: the versioned model has two nodes
        n_models += 1  # int_ref_seed_countries
        shape = {
            "models": n_models,
            "by_layer": {l: sum(1 for m in self.models.values() if m.layer == l) for l in ("staging", "intermediate", "marts")},
            "sources": len(self.sources),
            "exposures": sum(len(v) for v in exposures.values()),
            "yaml_tests": n_tests,
            "seeds": 2, "snapshots": 1, "disabled": 1,
        }
        (out / "shape.json").write_text(json.dumps(shape, indent=2))
        return shape


def generate(n_models: int, out: Path, seed: int = 7, yaml_style: str = "core") -> dict:
    # Two nodes are added outside the layer loop (the second version of the
    # versioned mart and the seed-reading model), so ask for n - 1 layered models.
    g = Gen(n_models - 1, seed, yaml_style)
    g.build()
    return g.write(out)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--models", type=int, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--yaml-style", choices=["core", "v2"], default="core",
                    help="core: flat generic-test args and top-level source freshness (dbt 1.8-1.12); "
                         "v2: test args under 'arguments:' and source freshness under 'config:' "
                         "(required by dbt v2 / Fusion, also accepted by dbt 1.12)")
    a = ap.parse_args()
    print(json.dumps(generate(a.models, a.out, a.seed, a.yaml_style), indent=2))


if __name__ == "__main__":
    main()
