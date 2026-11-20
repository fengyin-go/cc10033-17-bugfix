"""层级构建服务：构建维度表的多级层级关系，支持父子、路径和桥接表三种模式。

增强能力：
- 消费 dim.published/attr.confirmed 通道获取层级构建上下文
- 三种层级模式：父子字段(parent_id)、层级路径(path_string)、桥接表(bridge_table)
- 递归深度检测：防止循环引用
- 扁平化转换：将树形层级转换为列式属性
- 层级发布切换：重建层级时，新树必须通过父节点与路径完整性校验才能接管
  当前层级；切换将生效层级路径数据与桥接事实分组一并落库，旧树转为已重建
  并保留原层级编号，历史桥接分摊仍可按旧父节点复算
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

LEVEL_MODES = {"父子字段", "层级路径", "桥接表"}


class HierarchyBuildService(BaseModuleService):
    MODULE = "hierarchy_build"
    ENTITY = "层级关系"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["层级名称", "所属维度", "父子字段", "层级路径", "递归深度", "桥接表", "扁平化"]
    REQUIRED_FIELDS = ["层级名称", "所属维度", "父子字段"]
    STATUS_ORDER = ["草稿", "已构建", "已生效", "已重建"]
    ACTION_RULES = {"构建层级": "草稿", "重建层级": "已生效"}
    NEGATIVE_ACTIONS = []

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        dim_code = str(values.get("所属维度", "")).strip()
        max_depth = _safe_int(values.get("递归深度"), 3)

        # 消费维度发布通道
        dim_msgs = store.channel.consume("dim.published", "hierarchy_build")
        for msg in dim_msgs:
            if msg["payload"].get("dim_code") == dim_code:
                values["_dim_source"] = msg["payload"].get("source", "")

        # 检测层级模式
        parent_field = str(values.get("父子字段", ""))
        bridge_table = str(values.get("桥接表", ""))
        hierarchy_path = str(values.get("层级路径", ""))
        if bridge_table:
            mode = "桥接表"
        elif "/" in hierarchy_path:
            mode = "层级路径"
        elif parent_field:
            mode = "父子字段"
        else:
            mode = "父子字段"

        # 递归深度安全检查
        if max_depth > 10:
            max_depth = 10
            values["递归深度"] = "10"

        # 检查是否存在已生效的相同维度层级
        for row in store.rows(self.MODULE):
            if row.get("所属维度") == dim_code and row.get("status") == "已生效":
                values["_prev_hierarchy_exists"] = True
                break

        entry, missing = super().create_entry(values)
        if entry is None:
            return None, missing

        # 新登记的层级尚未生效，版本号待发布切换时分配
        entry["_version"] = 0
        entry["_mode"] = mode

        store.channel.publish("hierarchy.defined", {
            "hierarchy_name": entry["层级名称"],
            "dim_code": dim_code,
            "mode": mode,
            "max_depth": max_depth,
            "entry_id": entry["id"],
        })

        store.artifacts.register(
            "hierarchy_build", "hierarchy_def",
            f"hierarchy_{entry['id']}",
            {"hierarchy_name": entry["层级名称"], "dim_code": dim_code, "mode": mode},
            ["dim_definition", "hierarchy_build"],
        )
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "构建层级":
            return self._build(entry_id)
        if action == "重建层级":
            return self._rebuild(entry_id)
        return super().run_action(entry_id, action)

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = super().get_entry(entry_id)
        if entry is not None:
            version = _safe_int(entry.get("_version"), 0)
            entry["_version"] = version
            # 该层级版本已落库的路径数据（含历史版本，供复算核对）
            entry["_paths_persisted"] = [
                row for row in store.rows("hierarchy_paths")
                if row.get("所属维度") == entry.get("所属维度")
                and _safe_int(row.get("层级版本"), -1) == version
            ]
        return entry

    # ------------------------------------------------------------------
    # 构建层级：草稿 -> 已构建（只构建校验，不接管当前层级）
    # ------------------------------------------------------------------
    def _build(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if entry.get("status") != "草稿":
            return None, f"仅草稿状态的层级可执行构建层级，当前状态：{entry.get('status')}"

        dim_code = str(entry.get("所属维度", ""))
        max_depth = _safe_int(entry.get("递归深度"), 3)
        tree = _load_member_tree(dim_code)
        if tree:
            errors = _validate_tree(tree, max_depth)
            if errors:
                return None, "层级校验未通过：" + "；".join(errors)
            entry["_tree"] = tree
            entry["_paths"] = _compute_paths(tree)
            node_count = len(tree)
        else:
            # 无成员源数据时保留模拟构建
            node_count = _simulate_tree(entry)["node_count"]

        entry["status"] = "已构建"
        entry["pending"] = True
        entry["abnormal"] = False
        store.channel.publish("hierarchy.built", {
            "hierarchy_name": entry["层级名称"],
            "tree_nodes": node_count,
            "entry_id": entry_id,
        })
        return entry, f"{self.ENTITY}已构建层级"

    # ------------------------------------------------------------------
    # 重建层级：层级发布切换。新树通过父节点与路径完整性校验后接管当前
    # 层级，生效路径数据与桥接事实分组一并落库；旧树转为已重建并保留
    # 原层级编号，历史分摊仍可按旧父节点复算。
    # ------------------------------------------------------------------
    def _rebuild(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if entry.get("status") != "已生效":
            return None, f"仅已生效的层级可执行重建层级，当前状态：{entry.get('status')}"

        dim_code = str(entry.get("所属维度", ""))
        max_depth = _safe_int(entry.get("递归深度"), 3)

        # 从成员源读取最新父子关系，构建新树
        new_tree = _load_member_tree(dim_code)
        if not new_tree:
            return None, f"维度 {dim_code} 无成员源数据，无法重建层级"

        # 新树必须通过父节点与路径完整性校验，否则阻止切换
        errors = _validate_tree(new_tree, max_depth)
        if errors:
            store.channel.publish("hierarchy.rebuild_rejected", {
                "hierarchy_name": entry.get("层级名称"),
                "dim_code": dim_code,
                "errors": errors,
                "entry_id": entry_id,
            })
            return None, "新树未通过父节点/路径完整性校验，已阻止切换：" + "；".join(errors)

        old_version = _safe_int(entry.get("_version"), 1)
        new_version = old_version + 1
        now = _now()

        # 切换检查点：路径数据与桥接分组在同一检查点内落库
        cp_id = store.recovery.create_checkpoint(self.MODULE, f"switch_{entry_id}", {
            "dim_code": dim_code,
            "old_version": old_version,
            "new_version": new_version,
            "phase": "switch_started",
        })

        # 1. 旧树转为已重建（保留原层级编号与路径数据，供历史复算）
        entry["status"] = "已重建"
        entry["pending"] = False
        entry["_superseded_by"] = new_version
        entry["_switched_at"] = now

        # 2. 新树接管当前层级
        rows = store.rows(self.MODULE)
        new_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
        new_entry: dict[str, Any] = {
            "id": new_id,
            "层级名称": entry.get("层级名称"),
            "所属维度": dim_code,
            "父子字段": entry.get("父子字段"),
            "层级路径": entry.get("层级路径"),
            "递归深度": entry.get("递归深度"),
            "桥接表": entry.get("桥接表", ""),
            "扁平化": "否",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
            "_version": new_version,
            "_mode": entry.get("_mode", "父子字段"),
            "_tree": new_tree,
            "_paths": _compute_paths(new_tree),
            "_rebuilt_from": entry_id,
            "_switched_at": now,
        }
        rows.append(new_entry)

        # 3. 生效层级路径数据落库：旧版本路径保留但取消当前标记，新版本写入
        _persist_hierarchy_paths(entry, new_entry, new_tree, now)

        # 4. 桥接事实分组一并落库：在途批按原层级收尾确认，新批按新层级展开
        from app.services.dim_bridge import handle_hierarchy_switch
        switch_result = handle_hierarchy_switch(
            dim_code=dim_code,
            hierarchy_name=str(entry.get("层级名称", "")),
            old_version=old_version,
            new_version=new_version,
            switched_at=now,
        )

        # 5. 版本快照（审计追溯）
        store.snapshot.create_version_snapshot(
            self.MODULE, entry_id, dict(entry), f"v{old_version}")
        store.snapshot.create_version_snapshot(
            self.MODULE, new_id, dict(new_entry), f"v{new_version}")

        store.recovery.update_checkpoint(cp_id, {"phase": "switch_persisted"})
        store.recovery.mark_completed(cp_id)

        store.channel.publish("hierarchy.rebuilt", {
            "hierarchy_name": entry.get("层级名称"),
            "entry_id": entry_id,
            "old_version": old_version,
            "new_version": new_version,
        })
        store.channel.publish("hierarchy.switched", {
            "dim_code": dim_code,
            "old_version": old_version,
            "new_version": new_version,
            "old_entry_id": entry_id,
            "new_entry_id": new_id,
            "confirmed_batches": switch_result["confirmed_batches"],
            "started_batches": switch_result["started_batches"],
        })

        store.artifacts.register(
            "hierarchy_build", "hierarchy_paths",
            f"hierarchy_paths_{new_id}",
            {"hierarchy_name": new_entry["层级名称"], "dim_code": dim_code,
             "version": new_version, "node_count": len(new_tree)},
            ["hierarchy_build", "dim_bridge"],
        )

        message = (
            f"层级已重建并切换：{dim_code} v{old_version}→v{new_version}，旧树转为已重建；"
            f"{switch_result['confirmed_batches']} 个在途桥接批按 v{old_version} 收尾确认，"
            f"{switch_result['started_batches']} 个新批按 v{new_version} 展开"
        )
        return new_entry, message

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        mode_dist: dict[str, int] = {}
        for row in store.rows(self.MODULE):
            mode = row.get("_mode", "未知")
            mode_dist[mode] = mode_dist.get(mode, 0) + 1
        base["mode_distribution"] = mode_dist
        base["consumed_dims"] = len(store.channel.consume("dim.published", "hierarchy_build"))
        base["effective_versions"] = {
            row.get("所属维度"): _safe_int(row.get("_version"), 1)
            for row in store.rows(self.MODULE)
            if row.get("status") == "已生效"
        }
        base["persisted_paths"] = len(store.rows("hierarchy_paths"))
        return base


# ----------------------------------------------------------------------
# 层级树构建、校验与路径落库
# ----------------------------------------------------------------------

def _safe_int(value: Any, default: int) -> int:
    try:
        return int(str(value).strip())
    except (ValueError, TypeError):
        return default


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _load_member_tree(dim_code: str) -> dict[str, str]:
    """从维度成员源读取最新父子关系 {成员: 父成员}。"""
    tree: dict[str, str] = {}
    for row in store.rows("dim_member_source"):
        if row.get("所属维度") == dim_code:
            tree[str(row.get("成员", ""))] = str(row.get("父成员", ""))
    return tree


def _validate_tree(tree: dict[str, str], max_depth: int) -> list[str]:
    """父节点完整性与路径完整性校验。

    父节点完整性：单根、每个父节点都存在、无循环引用。
    路径完整性：每个节点都能回溯到根、路径深度不超过递归深度、层级无断档。
    """
    errors: list[str] = []
    if not tree:
        return ["成员树为空"]

    roots = [member for member, parent in tree.items() if not parent]
    if len(roots) != 1:
        errors.append(f"根节点数量异常：{len(roots)} 个（应为 1 个）")

    missing = sorted({parent for parent in tree.values() if parent and parent not in tree})
    if missing:
        errors.append(f"父节点缺失：{'、'.join(missing)}")

    if errors:
        return errors

    depths: set[int] = set()
    for member in tree:
        seen: set[str] = set()
        node = member
        depth = 0
        cyclic = False
        while node:
            if node in seen:
                errors.append(f"节点 {member} 存在循环引用")
                cyclic = True
                break
            seen.add(node)
            depth += 1
            node = tree.get(node, "")
        if cyclic:
            continue
        depths.add(depth)
        if depth > max_depth:
            errors.append(f"节点 {member} 路径深度 {depth} 超过递归深度 {max_depth}")

    if depths and depths != set(range(1, max(depths) + 1)):
        errors.append("路径层级存在断档，层级不完整")
    return errors


def _compute_paths(tree: dict[str, str]) -> dict[str, dict[str, Any]]:
    """计算每个节点的完整路径（根→节点），假定树已通过校验。"""
    paths: dict[str, dict[str, Any]] = {}
    for member, parent in tree.items():
        chain: list[str] = []
        node = member
        while node:
            chain.append(node)
            node = tree.get(node, "")
        chain.reverse()
        paths[member] = {
            "父节点": parent,
            "节点路径": "/".join(chain),
            "深度": len(chain),
        }
    return paths


def _persist_hierarchy_paths(
    old_entry: dict[str, Any],
    new_entry: dict[str, Any],
    new_tree: dict[str, str],
    now: str,
) -> None:
    """生效层级路径数据落库：旧版本路径保留（取消当前标记），新版本写入并标记当前。"""
    rows = store.rows("hierarchy_paths")
    dim_code = str(new_entry.get("所属维度", ""))
    old_version = _safe_int(old_entry.get("_version"), 1)
    new_version = _safe_int(new_entry.get("_version"), 1)

    # 旧版本路径保留原层级编号，仅取消当前标记，历史分摊仍可按旧父节点复算
    for row in rows:
        if (row.get("所属维度") == dim_code
                and _safe_int(row.get("层级版本"), -1) == old_version
                and row.get("是否当前") == "是"):
            row["是否当前"] = "否"
            row["_退役时间"] = now

    paths = _compute_paths(new_tree)
    max_id = max((int(row.get("id", 0)) for row in rows), default=0)
    for node, info in paths.items():
        max_id += 1
        rows.append({
            "id": max_id,
            "层级ID": new_entry["id"],
            "层级名称": new_entry.get("层级名称"),
            "所属维度": dim_code,
            "层级版本": new_version,
            "节点": node,
            "父节点": info["父节点"],
            "节点路径": info["节点路径"],
            "深度": info["深度"],
            "是否当前": "是",
            "_生效时间": now,
        })


def _simulate_tree(entry: dict[str, Any]) -> dict[str, Any]:
    """无成员源数据时的模拟树（兼容旧行为）。"""
    dim_code = entry.get("所属维度", "")
    max_depth = _safe_int(entry.get("递归深度"), 3)
    nodes = sum(pow(2, d) for d in range(max_depth))
    return {"root": dim_code, "node_count": nodes, "max_depth": max_depth,
            "mode": entry.get("_mode", "父子字段")}
