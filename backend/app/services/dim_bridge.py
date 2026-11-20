"""桥接关联服务：管理维度与事实之间的多对多桥接表定义。

增强能力：
- 消费 dim.published 和 fact.published 通道校验多对多关系
- 权重字段验证（权重值范围）
- 时效性管理：生效/失效时间与主记录标记
- 桥接分摊批：用生效层级路径数据展开事实，把实际父节点路径和层级版本
  写入分摊结果，同一笔事实只归属一个层级版本，避免跨树分组
- 层级发布切换联动：在途批按原层级版本收尾确认（保留原层级编号，历史
  金额可按旧父节点复算），切换后开始的新批按新层级版本展开
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class DimBridgeService(BaseModuleService):
    MODULE = "dim_bridge"
    ENTITY = "桥接表"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["桥接名称", "多对多维度", "关联事实", "权重字段", "生效时间", "失效时间", "主记录标记"]
    REQUIRED_FIELDS = ["桥接名称", "多对多维度", "关联事实"]
    STATUS_ORDER = ["草稿", "已生效", "已失效"]
    ACTION_RULES = {"启用桥接": "草稿", "停用桥接": "已生效"}
    NEGATIVE_ACTIONS = ["停用桥接"]

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        many_dims = str(values.get("多对多维度", "")).strip()
        related_fact = str(values.get("关联事实", "")).strip()

        # 校验维度存在性
        _validate_bridge_dims(many_dims, related_fact)

        # 设置默认时间
        if not values.get("生效时间"):
            values["生效时间"] = "2026-01-01"
        if not values.get("失效时间"):
            values["失效时间"] = "9999-12-31"
        if not values.get("主记录标记"):
            values["主记录标记"] = "Y"

        # 消费通道数据
        _consume_bridge_context(many_dims, related_fact)
        entry, missing = super().create_entry(values)
        if entry is None:
            return None, missing

        store.channel.publish("bridge.defined", {
            "bridge_name": entry["桥接名称"],
            "dims": many_dims,
            "fact": related_fact,
            "entry_id": entry["id"],
        })

        store.artifacts.register(
            "dim_bridge", "bridge_table",
            f"bridge_{entry['id']}",
            {"bridge_name": entry["桥接名称"], "dims": many_dims, "fact": related_fact},
            ["dim_definition", "fact_design", "dim_bridge"],
        )
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action not in self.ACTION_RULES:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        expected = self.ACTION_RULES[action]
        if entry.get("status") != expected:
            return None, f"仅{expected}状态的{self.ENTITY}可执行{action}，当前状态：{entry.get('status')}"
        if action == "启用桥接":
            return self._activate(entry)
        return self._deactivate(entry)

    def _activate(self, entry: dict[str, Any]) -> tuple[dict[str, Any], str]:
        now = _now()
        entry["status"] = "已生效"
        entry["pending"] = False
        entry["abnormal"] = False
        store.channel.publish("bridge.activated", {
            "bridge_name": entry["桥接名称"],
            "entry_id": entry["id"],
        })

        # 桥接事实分组落库，并按当前生效层级启动首个分摊批
        batch_info = ""
        dim_code = _hierarchy_dim_of(entry)
        if dim_code:
            version = _effective_hierarchy_version(dim_code)
            if version is not None:
                if _current_grouping_version(entry["id"], dim_code) is None:
                    _append_grouping(entry, dim_code, version, now)
                batch = start_batch(entry, started_at=now)
                if batch is not None:
                    batch_info = f"，分摊批 {batch['批次号']} 已按层级 v{batch['层级版本']} 展开"
        return entry, f"{self.ENTITY}已启用桥接{batch_info}"

    def _deactivate(self, entry: dict[str, Any]) -> tuple[dict[str, Any], str]:
        entry["status"] = "已失效"
        entry["pending"] = False
        entry["abnormal"] = True
        entry["失效时间"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        store.channel.publish("bridge.deactivated", {
            "bridge_name": entry["桥接名称"],
            "entry_id": entry["id"],
        })
        return entry, f"{self.ENTITY}已停用桥接"

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        items, total = super().list_entries(
            filters=filters, keyword=keyword, status=status, page=page, size=size)
        for row in items:
            _enrich_bridge_row(row)
        return items, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = super().get_entry(entry_id)
        if entry is not None:
            _enrich_bridge_row(entry)
            # 分摊批明细：在途批的分摊归属与状态、切换后开始的新批一并返回
            batches = [b for b in store.rows("bridge_batches")
                       if int(b.get("桥接ID", 0)) == int(entry_id)]
            entry["_batches"] = [
                {
                    **batch,
                    "_allocations": [
                        alloc for alloc in store.rows("bridge_allocations")
                        if alloc.get("批次号") == batch.get("批次号")
                    ],
                    "_复算": recompute_batch(batch["批次号"]),
                }
                for batch in batches
            ]
        return entry

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        active = sum(1 for r in store.rows(self.MODULE) if r.get("status") == "已生效")
        base["active_bridges"] = active
        batches = store.rows("bridge_batches")
        base["batches"] = {
            "在途": sum(1 for b in batches if b.get("状态") == "在途"),
            "已确认": sum(1 for b in batches if b.get("状态") == "已确认"),
        }
        # 已确认批须能由其标注的层级完整复算
        base["confirmed_recompute"] = {
            b["批次号"]: recompute_batch(b["批次号"])["可复算"]
            for b in batches if b.get("状态") == "已确认"
        }
        return base


# ----------------------------------------------------------------------
# 桥接分摊批引擎：按生效层级路径展开事实、版本切换与复算
# ----------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _bridge_dims(bridge: dict[str, Any]) -> list[str]:
    return [d.strip() for d in str(bridge.get("多对多维度", "")).split(".") if d.strip()]


def _hierarchy_dim_of(bridge: dict[str, Any]) -> str | None:
    """桥接用于展开事实的层级维度：多对多维度中首个已落库层级路径的维度。"""
    for dim in _bridge_dims(bridge):
        for row in store.rows("hierarchy_paths"):
            if row.get("所属维度") == dim:
                return dim
    return None


def _paths_by_version(dim_code: str, version: int) -> dict[str, dict[str, Any]]:
    """读取指定层级版本已落库的路径数据（含历史版本，供复算）。"""
    paths: dict[str, dict[str, Any]] = {}
    for row in store.rows("hierarchy_paths"):
        if (row.get("所属维度") == dim_code
                and int(row.get("层级版本", 0)) == int(version)):
            paths[str(row.get("节点", ""))] = row
    return paths


def _effective_hierarchy_version(dim_code: str) -> int | None:
    """当前生效的层级版本。"""
    for row in store.rows("hierarchy_paths"):
        if row.get("所属维度") == dim_code and row.get("是否当前") == "是":
            return int(row.get("层级版本", 0))
    return None


def _hierarchy_name(dim_code: str, version: int) -> str:
    for row in store.rows("hierarchy_paths"):
        if (row.get("所属维度") == dim_code
                and int(row.get("层级版本", 0)) == int(version)):
            return str(row.get("层级名称", ""))
    return ""


def _current_grouping_version(bridge_id: int, dim_code: str) -> int | None:
    """桥接当前的事实分组层级版本（落库的桥接事实分组）。"""
    for row in store.rows("bridge_grouping"):
        if (int(row.get("桥接ID", 0)) == int(bridge_id)
                and row.get("层级维度") == dim_code
                and row.get("是否当前") == "是"):
            return int(row.get("层级版本", 0))
    return None


def _append_grouping(bridge: dict[str, Any], dim_code: str, version: int, switched_at: str) -> None:
    rows = store.rows("bridge_grouping")
    rows.append({
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "桥接ID": bridge["id"],
        "桥接名称": bridge.get("桥接名称", ""),
        "层级维度": dim_code,
        "层级版本": int(version),
        "是否当前": "是",
        "切换时间": switched_at,
    })


def _bridge_facts(bridge: dict[str, Any]) -> list[dict[str, Any]]:
    fact_name = str(bridge.get("关联事实", ""))
    return [r for r in store.rows("bridge_fact_source") if r.get("关联事实") == fact_name]


def _allocated_fact_ids(bridge_id: int) -> set[str]:
    return {
        str(a.get("事实ID"))
        for a in store.rows("bridge_allocations")
        if int(a.get("桥接ID", 0)) == int(bridge_id)
    }


def _next_batch_no() -> str:
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    seq = sum(
        1 for b in store.rows("bridge_batches")
        if str(b.get("批次号", "")).startswith(f"BRB-{today}")
    ) + 1
    return f"BRB-{today}-{seq:03d}"


def start_batch(
    bridge: dict[str, Any],
    version: int | None = None,
    started_at: str | None = None,
) -> dict[str, Any] | None:
    """按生效层级版本启动分摊批：展开未分摊事实，写入实际父节点路径和层级版本。"""
    dim_code = _hierarchy_dim_of(bridge)
    if dim_code is None:
        return None
    if version is None:
        version = (_current_grouping_version(bridge["id"], dim_code)
                   or _effective_hierarchy_version(dim_code))
    if version is None:
        return None
    paths = _paths_by_version(dim_code, version)
    allocated = _allocated_fact_ids(bridge["id"])
    facts = [
        f for f in _bridge_facts(bridge)
        if str(f.get("事实ID")) not in allocated and str(f.get("成员")) in paths
    ]
    if not facts:
        return None

    batches = store.rows("bridge_batches")
    batch: dict[str, Any] = {
        "id": max((int(b.get("id", 0)) for b in batches), default=0) + 1,
        "批次号": _next_batch_no(),
        "桥接ID": bridge["id"],
        "桥接名称": bridge.get("桥接名称", ""),
        "关联事实": bridge.get("关联事实", ""),
        "层级维度": dim_code,
        "层级名称": _hierarchy_name(dim_code, version),
        "层级版本": int(version),
        "状态": "在途",
        "事实清单": [str(f.get("事实ID")) for f in facts],
        "开始时间": started_at or _now(),
        "确认时间": "",
    }
    batches.append(batch)
    _expand_facts(bridge, batch, facts, paths)
    return batch


def _expand_facts(
    bridge: dict[str, Any],
    batch: dict[str, Any],
    facts: list[dict[str, Any]],
    paths: dict[str, dict[str, Any]],
) -> None:
    """用批次标注的层级版本路径展开事实：实际父节点路径和层级版本写入分摊结果，
    成员不在该版本层级中的事实直接跳过，保证一笔事实不跨树分组。"""
    rows = store.rows("bridge_allocations")
    max_id = max((int(r.get("id", 0)) for r in rows), default=0)
    for fact in facts:
        member = str(fact.get("成员", ""))
        node = paths.get(member)
        if node is None:
            continue
        max_id += 1
        amount = float(fact.get("金额", 0) or 0)
        weight = 1.0
        rows.append({
            "id": max_id,
            "批次号": batch["批次号"],
            "桥接ID": bridge["id"],
            "事实ID": str(fact.get("事实ID")),
            "成员": member,
            "父节点": node.get("父节点", ""),
            "父节点路径": node.get("节点路径", ""),
            "层级版本": int(batch["层级版本"]),
            "权重": weight,
            "金额": amount,
            "分摊金额": round(amount * weight, 2),
        })


def confirm_batch(batch: dict[str, Any], confirmed_at: str | None = None) -> None:
    """确认批次：剩余事实仍按批次标注的层级版本展开后收尾，输出可按标注层级复算。"""
    bridge = store.find("dim_bridge", int(batch.get("桥接ID", 0)))
    if bridge is not None:
        paths = _paths_by_version(str(batch.get("层级维度", "")), int(batch.get("层级版本", 0)))
        done = {
            str(a.get("事实ID"))
            for a in store.rows("bridge_allocations")
            if a.get("批次号") == batch.get("批次号")
        }
        remaining = [
            f for f in _bridge_facts(bridge)
            if str(f.get("事实ID")) in batch.get("事实清单", [])
            and str(f.get("事实ID")) not in done
        ]
        if remaining:
            _expand_facts(bridge, batch, remaining, paths)
    batch["状态"] = "已确认"
    batch["确认时间"] = confirmed_at or _now()


def handle_hierarchy_switch(
    dim_code: str,
    hierarchy_name: str,
    old_version: int,
    new_version: int,
    switched_at: str | None = None,
) -> dict[str, Any]:
    """层级发布切换时，桥接事实分组一并落库。

    1. 在途批按原层级版本收尾确认：已完成的桥接分摊保留原层级编号，
       历史金额可按旧父节点复算；
    2. 桥接事实分组切换到新层级版本（落库）；
    3. 未分摊事实按新层级版本启动新批。
    """
    switched_at = switched_at or _now()
    result: dict[str, Any] = {"confirmed_batches": 0, "started_batches": 0, "bridges": []}

    for bridge in store.rows("dim_bridge"):
        if bridge.get("status") != "已生效":
            continue
        if dim_code not in _bridge_dims(bridge):
            continue

        # 1. 在途批按原层级收尾确认（保留原层级编号）
        for batch in store.rows("bridge_batches"):
            if (int(batch.get("桥接ID", 0)) == int(bridge["id"])
                    and batch.get("状态") == "在途"
                    and int(batch.get("层级版本", 0)) == int(old_version)):
                confirm_batch(batch, confirmed_at=switched_at)
                result["confirmed_batches"] += 1
                store.channel.publish("bridge.batch_confirmed", {
                    "batch_no": batch["批次号"],
                    "bridge_id": bridge["id"],
                    "hierarchy_version": batch["层级版本"],
                })

        # 2. 桥接事实分组切换落库
        for row in store.rows("bridge_grouping"):
            if (int(row.get("桥接ID", 0)) == int(bridge["id"])
                    and row.get("层级维度") == dim_code
                    and row.get("是否当前") == "是"):
                row["是否当前"] = "否"
        _append_grouping(bridge, dim_code, new_version, switched_at)
        store.channel.publish("bridge.grouping_switched", {
            "bridge_id": bridge["id"],
            "dim_code": dim_code,
            "old_version": old_version,
            "new_version": new_version,
        })

        # 3. 切换后开始的新批按新层级展开
        batch = start_batch(bridge, version=new_version, started_at=switched_at)
        if batch is not None:
            result["started_batches"] += 1
            store.channel.publish("bridge.batch_started", {
                "batch_no": batch["批次号"],
                "bridge_id": bridge["id"],
                "hierarchy_version": new_version,
            })

        result["bridges"].append(bridge.get("桥接名称"))
    return result


def recompute_batch(batch_no: str) -> dict[str, Any]:
    """按批次标注的层级版本复算分摊输出：最终确认的桥接输出须能由其
    标注的层级完整复算。"""
    batch = next(
        (b for b in store.rows("bridge_batches") if b.get("批次号") == batch_no), None)
    if batch is None:
        return {"批次号": batch_no, "可复算": False, "差异": ["批次不存在"]}

    paths = _paths_by_version(str(batch.get("层级维度", "")), int(batch.get("层级版本", 0)))
    allocations = [a for a in store.rows("bridge_allocations") if a.get("批次号") == batch_no]

    mismatches: list[dict[str, Any]] = []
    groups: dict[str, float] = {}
    for alloc in allocations:
        node = paths.get(str(alloc.get("成员", "")))
        expected_parent = node.get("父节点") if node else None
        expected_path = node.get("节点路径") if node else None
        if expected_parent != alloc.get("父节点") or expected_path != alloc.get("父节点路径"):
            mismatches.append({
                "事实ID": alloc.get("事实ID"),
                "记录父节点": alloc.get("父节点"),
                "复算父节点": expected_parent,
            })
        key = expected_parent or "未匹配"
        groups[key] = round(groups.get(key, 0.0) + float(alloc.get("分摊金额", 0) or 0), 2)

    return {
        "批次号": batch_no,
        "层级版本": batch.get("层级版本"),
        "状态": batch.get("状态"),
        "分摊笔数": len(allocations),
        "可复算": not mismatches,
        "分组汇总": groups,
        "差异": mismatches,
    }


def _enrich_bridge_row(row: dict[str, Any]) -> None:
    """桥接列表/详情附加当前层级版本与批次状态，便于核对在途批归属。"""
    bridge_id = int(row.get("id", 0))
    batches = [b for b in store.rows("bridge_batches")
               if int(b.get("桥接ID", 0)) == bridge_id]
    dim_code = _hierarchy_dim_of(row)
    row["_层级维度"] = dim_code or ""
    row["_当前层级版本"] = (
        _current_grouping_version(bridge_id, dim_code) if dim_code else None
    ) or ""
    row["_在途批数"] = sum(1 for b in batches if b.get("状态") == "在途")
    row["_已确认批数"] = sum(1 for b in batches if b.get("状态") == "已确认")


def _validate_bridge_dims(dim_str: str, fact_str: str) -> None:
    dims = [d.strip() for d in dim_str.split(".") if d.strip()]
    existing = {r.get("维度编码") for r in store.rows("dim_definition")}
    valid = [d for d in dims if d in existing]
    if valid:
        store.channel.publish("bridge.dims_validated", {"valid_dims": valid, "dim_str": dim_str})


def _consume_bridge_context(dim_str: str, fact_str: str) -> None:
    store.channel.consume("dim.published", "dim_bridge")
    store.channel.consume("fact.published", "dim_bridge")
