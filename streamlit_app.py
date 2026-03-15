#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
预算分析 Streamlit 应用
- 下载 Excel 模板（来源：GitHub 或本地 templates/）
- 上传数据文件并运行分析
- 下载结果 Excel
"""

import os
import tempfile
from pathlib import Path

import streamlit as st

# 模板来源：优先本地 templates/，否则从 GitHub Raw 下载
# 下方写死你的 GitHub 模板地址；若设置了环境变量 BUDGET_TEMPLATE_URL 则优先用环境变量
GITHUB_TEMPLATE_URL_HARDCODE = "https://github.com/hihihidoraemon/20260315_Budget_operation/raw/main/20260314--数据文件模板.xlsx"
GITHUB_RAW_TEMPLATE_URL = os.environ.get("BUDGET_TEMPLATE_URL") or GITHUB_TEMPLATE_URL_HARDCODE

TEMPLATE_LOCAL_PATH = Path(__file__).parent / "templates" / "预算分析模板.xlsx"
TEMPLATE_FILENAME = "预算分析模板.xlsx"

st.set_page_config(page_title="预算分析", page_icon="📊", layout="centered")

st.title("📊 预算跟进分析")
st.caption("上传包含三个 sheet 的 Excel，运行分析后下载结果。")

# ---------- 1. 下载 Excel 模板 ----------
st.subheader("1️⃣ 下载 Excel 模板")
template_bytes = None
if TEMPLATE_LOCAL_PATH.exists():
    template_bytes = TEMPLATE_LOCAL_PATH.read_bytes()
    st.download_button(
        label="下载 Excel 模板",
        data=template_bytes,
        file_name=TEMPLATE_FILENAME,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
elif GITHUB_RAW_TEMPLATE_URL:
    try:
        import requests
        r = requests.get(GITHUB_RAW_TEMPLATE_URL, timeout=30)
        r.raise_for_status()
        template_bytes = r.content
        st.download_button(
            label="下载 Excel 模板（来自 GitHub）",
            data=template_bytes,
            file_name=TEMPLATE_FILENAME,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as e:
        st.warning(f"从 GitHub 拉取模板失败：{e}")
        st.markdown(f"可手动打开：[模板链接]({GITHUB_RAW_TEMPLATE_URL})")
else:
    st.info("请将模板放到仓库 `templates/预算分析模板.xlsx`，或设置环境变量 `BUDGET_TEMPLATE_URL` 为 GitHub Raw 地址。")

# ---------- 2. 上传文件 ----------
st.subheader("2️⃣ 上传数据文件")
uploaded = st.file_uploader(
    "上传包含「1--预算跟进表」「2--过去30天流水表」「3--事件数据表」的 Excel",
    type=["xlsx", "xls"],
)

# ---------- 3. 运行分析并下载结果 ----------
st.subheader("3️⃣ 运行分析并下载结果")
if "result_bytes" not in st.session_state:
    st.session_state.result_bytes = None

if uploaded:
    if st.button("运行分析"):
        try:
            from budget_analysis import main as run_analysis

            with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as fin:
                fin.write(uploaded.getvalue())
                input_path = fin.name
            with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as fout:
                output_path = fout.name

            with st.spinner("正在计算…"):
                run_analysis(input_path, output_path)

            with open(output_path, "rb") as f:
                st.session_state.result_bytes = f.read()

            os.remove(input_path)
            os.remove(output_path)
        except Exception as e:
            st.error(f"运行出错：{e}")
            raise

    if st.session_state.result_bytes is not None:
        st.success("分析完成，可下载结果文件。")
        st.download_button(
            label="下载分析结果 Excel",
            data=st.session_state.result_bytes,
            file_name="预算分析结果.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
else:
    st.session_state.result_bytes = None
    st.info("请先上传 Excel 文件后再点击「运行分析」。")
