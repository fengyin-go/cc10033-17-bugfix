"""层级发布切换与桥接分摊测试：重建层级后旧树转为已重建、新树校验后接管；
生效层级路径数据与桥接事实分组一并落库；在途批按原层级收尾、新批按新层级展开；
已确认批输出可由其标注的层级完整复算。"""
from __future__ import annotations

import pytest

from app.store import store
from app.services.hierarchy_build import HierarchyBuildService
from app.services.dim_bridge import (
    DimBridgeService,
    recompute_batch,
    _paths_by_version,
)


@pytest.fixture
def fresh_store():
    """每个测试重置全局 store，回到种子数据状态。"""
    store.__init__()
    return store


def _region_hierarchies():
    return [r for r in store.rows("hierarchy_build") if r.get("所属维度") == "DIM_STORE"]


def _batch(batch_no: str):
    return next(b for b in store.rows("bridge_batches") if b.get("批次号") == batch_no)


def _batch_allocs(batch_no: str):
    return [a for a in store.rows("bridge_allocations") if a.get("批次号") == batch_no]


# ======================================================================
# Section A: 层级发布切换
# ======================================================================


class TestHierarchySwitch:
    """重建层级：旧树已生效状态必须更新，新树校验后接管当前层级。"""

    def test_rebuild_switches_effective_version(self, fresh_store) -> None:
        """重建后旧树转为已重建，新树以新版本号接管已生效，同维度仅一个已生效。"""
        svc = HierarchyBuildService()
        entry, message = svc.run_action(2, "重建层级")
        assert entry is not None, message

        old = store.find("hierarchy_build", 2)
        assert old["status"] == "已重建"
        assert old["_version"] == 1  # 旧树保留原层级编号

        assert entry["status"] == "已生效"
        assert entry["_version"] == 2
        assert entry["_rebuilt_from"] == 2

        effective = [r for r in _region_hierarchies() if r.get("status") == "已生效"]
        assert len(effective) == 1
        assert effective[0]["id"] == entry["id"]

    def test_rebuild_persists_paths_and_flips_current_flag(self, fresh_store) -> None:
        """生效层级路径数据落库：v1 路径保留但取消当前标记，v2 写入并标记当前。"""
        svc = HierarchyBuildService()
        svc.run_action(2, "重建层级")

        paths = [r for r in store.rows("hierarchy_paths") if r.get("所属维度") == "DIM_STORE"]
        v1 = [r for r in paths if r.get("层级版本") == 1]
        v2 = [r for r in paths if r.get("层级版本") == 2]
        assert v1 and all(r.get("是否当前") == "否" for r in v1)  # 历史路径保留
        assert v2 and all(r.get("是否当前") == "是" for r in v2)

        v2_by_node = {r["节点"]: r for r in v2}
        assert v2_by_node["华东区域"]["父节点"] == "东部大区"
        assert v2_by_node["华东区域"]["节点路径"] == "全国/东部大区/华东区域"
        v1_by_node = {r["节点"]: r for r in v1}
        assert v1_by_node["华东区域"]["父节点"] == "华东大区"  # 旧父节点仍可复算

    def test_rebuild_blocked_when_parent_missing(self, fresh_store) -> None:
        """父节点缺失时新树不得接管，旧树保持已生效。"""
        store.rows("dim_member_source").append(
            {"id": 9001, "所属维度": "DIM_STORE", "成员": "华西区域", "父成员": "不存在的大区"})
        svc = HierarchyBuildService()
        entry, message = svc.run_action(2, "重建层级")
        assert entry is None
        assert "父节点缺失" in message
        assert store.find("hierarchy_build", 2)["status"] == "已生效"
        assert len(_region_hierarchies()) == 1  # 未产生新树

    def test_rebuild_blocked_when_cycle(self, fresh_store) -> None:
        """循环引用时阻止切换。"""
        for row in store.rows("dim_member_source"):
            if row.get("所属维度") == "DIM_STORE" and row.get("成员") == "华东区域":
                row["父成员"] = "上海"  # 上海 -> 华东区域 -> 上海 循环
        svc = HierarchyBuildService()
        entry, message = svc.run_action(2, "重建层级")
        assert entry is None
        assert "循环" in message
        assert store.find("hierarchy_build", 2)["status"] == "已生效"

    def test_rebuild_blocked_when_depth_exceeded(self, fresh_store) -> None:
        """路径深度超过递归深度时阻止切换。"""
        store.rows("dim_member_source").append(
            {"id": 9002, "所属维度": "DIM_STORE", "成员": "浦东", "父成员": "上海"})
        svc = HierarchyBuildService()
        entry, message = svc.run_action(2, "重建层级")
        assert entry is None
        assert "超过递归深度" in message

    def test_rebuild_requires_effective_status(self, fresh_store) -> None:
        """仅已生效的层级可重建。"""
        svc = HierarchyBuildService()
        entry, message = svc.run_action(3, "重建层级")  # 渠道分类层级为草稿
        assert entry is None
        assert "已生效" in message

    def test_build_action_moves_draft_to_built(self, fresh_store) -> None:
        """构建层级：草稿 -> 已构建，并通过成员源校验。"""
        svc = HierarchyBuildService()
        entry, message = svc.run_action(3, "构建层级")
        assert entry is not None, message
        assert entry["status"] == "已构建"
        assert entry["_tree"]["线上"] == "全渠道"


# ======================================================================
# Section B: 桥接事实分组随层级切换一并落库
# ======================================================================


class TestBridgeGroupingSwitch:
    """层级切换时桥接事实分组一并落库，在途批按原层级收尾。"""

    def test_inflight_batch_confirmed_with_original_version(self, fresh_store) -> None:
        """在途批按原层级版本收尾确认：已展开与剩余事实都保留 v1 和旧父节点。"""
        svc = HierarchyBuildService()
        svc.run_action(2, "重建层级")

        batch = _batch("BRB-20261118-001")
        assert batch["状态"] == "已确认"
        assert batch["层级版本"] == 1  # 保留原层级编号
        assert batch["确认时间"]

        allocs = _batch_allocs("BRB-20261118-001")
        assert len(allocs) == 6  # 剩余事实已在原层级下补齐
        assert all(a["层级版本"] == 1 for a in allocs)
        huadong = [a for a in allocs if a["成员"] == "华东区域"]
        assert all(a["父节点"] == "华东大区" for a in huadong)  # 旧父节点
        assert all(a["父节点路径"] == "全国/华东大区/华东区域" for a in huadong)
        assert sum(a["分摊金额"] for a in huadong) == 1800.0

    def test_new_batch_after_switch_uses_new_version(self, fresh_store) -> None:
        """切换后开始的新批按新层级版本展开，华东区域归属东部大区。"""
        svc = HierarchyBuildService()
        svc.run_action(2, "重建层级")

        new_batches = [b for b in store.rows("bridge_batches")
                       if b.get("桥接ID") == 5 and b.get("批次号") != "BRB-20261118-001"]
        assert len(new_batches) == 1
        batch = new_batches[0]
        assert batch["状态"] == "在途"
        assert batch["层级版本"] == 2

        allocs = _batch_allocs(batch["批次号"])
        assert {a["事实ID"] for a in allocs} == {"FACT-007", "FACT-008", "FACT-009"}
        huadong = [a for a in allocs if a["成员"] == "华东区域"]
        assert all(a["父节点"] == "东部大区" for a in huadong)
        assert all(a["父节点路径"] == "全国/东部大区/华东区域" for a in huadong)
        assert all(a["层级版本"] == 2 for a in allocs)

    def test_grouping_persisted_atomically(self, fresh_store) -> None:
        """桥接事实分组落库：当前分组切换到 v2，v1 分组保留为历史。"""
        svc = HierarchyBuildService()
        svc.run_action(2, "重建层级")

        grouping = [r for r in store.rows("bridge_grouping") if r.get("桥接ID") == 5]
        current = [r for r in grouping if r.get("是否当前") == "是"]
        history = [r for r in grouping if r.get("是否当前") == "否"]
        assert len(current) == 1 and current[0]["层级版本"] == 2
        assert len(history) == 1 and history[0]["层级版本"] == 1

    def test_fact_never_grouped_across_trees(self, fresh_store) -> None:
        """同一桥接下，一笔事实只归属一个层级版本，不跨树分组。"""
        svc = HierarchyBuildService()
        svc.run_action(2, "重建层级")

        seen: dict[str, int] = {}
        for alloc in store.rows("bridge_allocations"):
            if int(alloc.get("桥接ID", 0)) != 5:
                continue
            fact_id = alloc["事实ID"]
            assert fact_id not in seen, f"事实 {fact_id} 跨树重复分组"
            seen[fact_id] = alloc["层级版本"]

    def test_unaffected_bridge_not_touched(self, fresh_store) -> None:
        """不属于该维度层级的桥接不受区域树切换影响。"""
        svc = HierarchyBuildService()
        svc.run_action(2, "重建层级")

        product_grouping = [r for r in store.rows("bridge_grouping")
                            if r.get("桥接ID") == 1 and r.get("是否当前") == "是"]
        assert len(product_grouping) == 1
        assert product_grouping[0]["层级版本"] == 1  # 产品桥接分组未变


# ======================================================================
# Section C: 已确认批输出可按标注层级完整复算
# ======================================================================


class TestRecompute:
    """最终确认的桥接输出须能由其标注的层级完整复算。"""

    def test_confirmed_batch_recomputable_by_labeled_hierarchy(self, fresh_store) -> None:
        svc = HierarchyBuildService()
        svc.run_action(2, "重建层级")

        result = recompute_batch("BRB-20261118-001")
        assert result["状态"] == "已确认"
        assert result["层级版本"] == 1
        assert result["可复算"] is True
        assert result["分摊笔数"] == 6
        assert result["分组汇总"]["华东大区"] == 1800.0
        assert result["分组汇总"]["华南大区"] == 1400.0
        assert result["分组汇总"]["华北大区"] == 1200.0

    def test_old_batch_not_recomputable_by_new_tree(self, fresh_store) -> None:
        """同一成员在两个版本下父节点相异：旧批按新树复算必然不一致，
        说明已确认输出只能按其标注的层级版本复算。"""
        svc = HierarchyBuildService()
        svc.run_action(2, "重建层级")

        paths_v2 = _paths_by_version("DIM_STORE", 2)
        assert paths_v2["华东区域"]["父节点"] == "东部大区"
        mismatched = [
            a for a in _batch_allocs("BRB-20261118-001")
            if a["成员"] == "华东区域" and a["父节点"] != paths_v2["华东区域"]["父节点"]
        ]
        assert mismatched  # 旧批标注的父节点与新树不同

    def test_historical_amounts_recompute_by_old_parent(self, fresh_store) -> None:
        """已完成的桥接分摊保留原层级编号，历史金额按旧父节点复算。"""
        svc = HierarchyBuildService()
        svc.run_action(2, "重建层级")

        # 再重建一次（v2 -> v3），v1 的已确认分摊仍可按 v1 复算
        svc.run_action(5, "重建层级")
        result = recompute_batch("BRB-20261118-001")
        assert result["可复算"] is True
        assert result["层级版本"] == 1
        assert result["分组汇总"]["华东大区"] == 1800.0


# ======================================================================
# Section D: 启用桥接启动分摊批
# ======================================================================


class TestBridgeActivation:
    """启用桥接：分组落库并按当前生效层级启动分摊批。"""

    def test_activate_bridge_starts_batch_with_current_version(self, fresh_store) -> None:
        svc = DimBridgeService()
        entry, missing = svc.create_entry({
            "桥接名称": "门店-渠道测试桥接",
            "多对多维度": "DIM_STORE.DIM_CHANNEL",
            "关联事实": "销售明细事实",
            "权重字段": "region_weight",
            "生效时间": "",
            "失效时间": "",
            "主记录标记": "",
        })
        assert entry is not None, missing
        entry, message = svc.run_action(entry["id"], "启用桥接")
        assert entry is not None, message
        assert entry["status"] == "已生效"

        batches = [b for b in store.rows("bridge_batches") if b.get("桥接ID") == entry["id"]]
        assert len(batches) == 1
        assert batches[0]["层级版本"] == 1  # 当前生效层级
        assert batches[0]["状态"] == "在途"

        allocs = _batch_allocs(batches[0]["批次号"])
        assert allocs
        assert all(a["层级版本"] == 1 and a["父节点路径"] for a in allocs)

        grouping = [r for r in store.rows("bridge_grouping")
                    if r.get("桥接ID") == entry["id"] and r.get("是否当前") == "是"]
        assert len(grouping) == 1 and grouping[0]["层级版本"] == 1

    def test_activate_after_switch_uses_new_version(self, fresh_store) -> None:
        """层级切换后启用的桥接，分摊批按新层级版本展开。"""
        HierarchyBuildService().run_action(2, "重建层级")

        svc = DimBridgeService()
        entry, _ = svc.create_entry({
            "桥接名称": "门店-区域新桥接",
            "多对多维度": "DIM_STORE.DIM_CHANNEL",
            "关联事实": "销售明细事实",
            "权重字段": "region_weight",
            "生效时间": "",
            "失效时间": "",
            "主记录标记": "",
        })
        entry, _ = svc.run_action(entry["id"], "启用桥接")
        batches = [b for b in store.rows("bridge_batches") if b.get("桥接ID") == entry["id"]]
        assert len(batches) == 1
        assert batches[0]["层级版本"] == 2

    def test_deactivate_requires_effective(self, fresh_store) -> None:
        svc = DimBridgeService()
        entry, message = svc.run_action(2, "停用桥接")  # id=2 为草稿
        assert entry is None
        assert "已生效" in message

    def test_second_switch_confirms_previous_new_batch(self, fresh_store) -> None:
        """再次重建（v2→v3）：上一轮切换开始的新批按 v2 收尾确认。"""
        svc = HierarchyBuildService()
        svc.run_action(2, "重建层级")  # v1 -> v2：种子在途批收尾，新批按 v2 展开
        entry, message = svc.run_action(5, "重建层级")  # v2 -> v3
        assert entry is not None, message
        assert entry["_version"] == 3

        v2_batches = [b for b in store.rows("bridge_batches")
                      if b.get("桥接ID") == 5 and b.get("层级版本") == 2]
        assert len(v2_batches) == 1
        assert v2_batches[0]["状态"] == "已确认"  # 切换后开始的新批被下一轮切换收尾
        assert recompute_batch(v2_batches[0]["批次号"])["可复算"] is True

        # v1 已确认批保留原层级编号，仍可按旧父节点复算
        result = recompute_batch("BRB-20261118-001")
        assert result["可复算"] is True
        assert result["分组汇总"]["华东大区"] == 1800.0


# ======================================================================
# Section E: 列表与详情核对
# ======================================================================


class TestBridgeListEnrichment:
    """桥接列表可核对在途批的分摊归属和状态。"""

    def test_list_shows_batch_status(self, fresh_store) -> None:
        svc = DimBridgeService()
        items, _ = svc.list_entries(filters={"桥接名称": "门店-区域分摊桥接"})
        assert len(items) == 1
        row = items[0]
        assert row["_层级维度"] == "DIM_STORE"
        assert row["_当前层级版本"] == 1
        assert row["_在途批数"] == 1
        assert row["_已确认批数"] == 0

    def test_detail_after_switch_shows_both_batches(self, fresh_store) -> None:
        HierarchyBuildService().run_action(2, "重建层级")

        svc = DimBridgeService()
        entry = svc.get_entry(5)
        assert entry["_当前层级版本"] == 2
        assert entry["_在途批数"] == 1
        assert entry["_已确认批数"] == 1

        batches = {b["批次号"]: b for b in entry["_batches"]}
        old_batch = batches["BRB-20261118-001"]
        assert old_batch["状态"] == "已确认"
        assert old_batch["层级版本"] == 1
        assert old_batch["_复算"]["可复算"] is True
        old_allocs = {a["事实ID"]: a for a in old_batch["_allocations"]}
        assert old_allocs["FACT-005"]["父节点"] == "华东大区"

        new_batch = next(b for no, b in batches.items() if no != "BRB-20261118-001")
        assert new_batch["状态"] == "在途"
        assert new_batch["层级版本"] == 2
        new_allocs = {a["事实ID"]: a for a in new_batch["_allocations"]}
        assert new_allocs["FACT-007"]["父节点"] == "东部大区"
        assert new_allocs["FACT-007"]["父节点路径"] == "全国/东部大区/华东区域"
