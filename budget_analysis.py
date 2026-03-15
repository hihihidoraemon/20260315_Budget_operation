#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
预算跟进自动化分析脚本
读取包含三个 sheet 的 Excel，按业务逻辑计算并输出预算分析结果。

用法:
  python budget_analysis.py <输入Excel路径> [输出Excel路径]

依赖:
  pip install pandas openpyxl
"""

import pandas as pd
import numpy as np
import re
import sys
from pathlib import Path


# ─────────────────── 工具函数 ───────────────────

def extract_bracket_num(text):
    """从 '[259920]Flights...' 提取方括号内数字"""
    m = re.search(r'\[(\d+)\]', str(text))
    return int(m.group(1)) if m else np.nan


def extract_payin_num(text):
    """从 Payin 字段提取数值 ('$0.15' → 0.15)"""
    m = re.search(r'[\d.]+', str(text))
    return float(m.group()) if m else 0.0


def safe_div(numerator, denominator, default=0):
    """安全除法，分母为 0 时返回 default"""
    return np.where(denominator != 0, numerator / denominator, default)


def agg_event_str(df, group_cols, result_col,
                  event_col='event', num_col='event_num', rate_col='event_rate'):
    """将同组多个事件行合并为一个单元格（换行分隔）"""
    if isinstance(group_cols, str):
        group_cols = [group_cols]
    records = []
    for keys, gdf in df.groupby(group_cols):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(zip(group_cols, keys))
        parts = []
        for _, r in gdf.iterrows():
            parts.append(
                f"{r[event_col]}:event_num是{int(r[num_col])}，"
                f"event rate是{r[rate_col]:.2%}")
        row[result_col] = '；\n'.join(parts)
        records.append(row)
    return pd.DataFrame(records) if records else pd.DataFrame(columns=group_cols + [result_col])


def calc_affiliate_events(aff_df, event_df, prev_day, latest_day, conv_col):
    """
    计算下游 Affiliate 维度的 reject 和其他 event 指标。
    aff_df 需包含 Offer ID, Affiliate, conv_col 三列。
    返回添加了 reject / event 指标列的 aff_df。
    """
    has_aff = 'Affiliate' in event_df.columns

    # ── reject（前一天）──
    if has_aff:
        rej = (event_df[(event_df['Event'] == 'reject') & (event_df['Time'] == prev_day)]
               .groupby(['Offer ID', 'Affiliate']).size()
               .reset_index(name='reject_num'))
        aff_df = aff_df.merge(rej, on=['Offer ID', 'Affiliate'], how='left')
    else:
        aff_df['reject_num'] = np.nan

    aff_df['reject_num'] = aff_df['reject_num'].fillna(0)
    tc = aff_df[conv_col].fillna(0)
    aff_df['reject_rate'] = safe_div(aff_df['reject_num'], tc)
    aff_df = aff_df.rename(columns={
        'reject_num': '下游Affiliate前一天的reject_num',
        'reject_rate': '下游Affiliate前一天的reject_rate',
    })

    # ── 非 reject 事件（最近一天）──
    if has_aff:
        non_rej = (event_df[(event_df['Event'] != 'reject') & (event_df['Time'] == latest_day)]
                   .groupby(['Offer ID', 'Affiliate', 'Event']).size()
                   .reset_index(name='event_num'))
        non_rej = non_rej.merge(
            aff_df[['Offer ID', 'Affiliate', conv_col]].drop_duplicates(),
            on=['Offer ID', 'Affiliate'], how='inner')
        non_rej['event_rate'] = safe_div(non_rej['event_num'], non_rej[conv_col].fillna(0))
        non_rej = non_rej.rename(columns={'Event': 'event'})
        if len(non_rej):
            ev_str = agg_event_str(non_rej, ['Offer ID', 'Affiliate'],
                                   '下游Affiliate最近一天的事件数和事件率')
            aff_df = aff_df.merge(ev_str, on=['Offer ID', 'Affiliate'], how='left')
        else:
            aff_df['下游Affiliate最近一天的事件数和事件率'] = np.nan
    else:
        aff_df['下游Affiliate最近一天的事件数和事件率'] = np.nan

    return aff_df


# ─────────────────── 主流程 ───────────────────

def main(excel_path, output_path=None):
    if output_path is None:
        output_path = str(Path(excel_path).stem) + '_分析结果.xlsx'

    # ==================== Point 1：导入数据 ====================
    print('Point 1: 读取 Excel …')
    budget_df = pd.read_excel(excel_path, sheet_name='1--预算跟进表')
    flow_df   = pd.read_excel(excel_path, sheet_name='2--过去30天流水表')
    event_df  = pd.read_excel(excel_path, sheet_name='3--事件数据表')

    # ==================== Point 2：预处理事件数据 ====================
    print('Point 2: 预处理事件数据 …')

    # Step1: 日期转换 + reject 日期减一天
    event_df['Time'] = pd.to_datetime(event_df['Time']).dt.normalize()
    mask_rej = (event_df['Event'] == 'reject')
    event_df.loc[mask_rej, 'Time'] -= pd.Timedelta(days=1)

    # Step2: 从 Offer Name 提取 Offer ID
    event_df['Offer ID'] = event_df['Offer Name'].apply(extract_bracket_num)
    event_df = event_df.dropna(subset=['Offer ID'])
    event_df['Offer ID'] = event_df['Offer ID'].astype(int)

    # 兼容事件表 Affiliate 列名
    for c in ('Affiliate Name', 'affiliate', 'Sub ID'):
        if c in event_df.columns and 'Affiliate' not in event_df.columns:
            event_df = event_df.rename(columns={c: 'Affiliate'})
            break

    # ==================== Point 3：筛选有效 Offer ====================
    print('Point 3: 筛选有效 Offer …')
    flow_df['Time'] = pd.to_datetime(flow_df['Time']).dt.normalize()
    latest_day = flow_df['Time'].max()
    prev_day   = latest_day - pd.Timedelta(days=1)
    print(f'  最近一天 = {latest_day.date()}, 前一天 = {prev_day.date()}')

    active_ids = set(flow_df.loc[flow_df['Status'] == 'ACTIVE', 'Offer ID'])
    rev_latest = (flow_df[flow_df['Time'] == latest_day]
                  .groupby('Offer ID')['Total Revenue'].sum())
    rev_ids = set(rev_latest[rev_latest > 0].index)
    valid_ids = active_ids | rev_ids

    p3_want = ['Adv Offer ID', 'Offer ID', 'Status', 'GEO', 'Payin',
               'Total Caps', 'Advertiser', 'App ID']
    p3_cols = [c for c in p3_want if c in flow_df.columns]
    p3 = (flow_df[flow_df['Offer ID'].isin(valid_ids)][p3_cols]
          .drop_duplicates(subset=['Offer ID'], keep='first')
          .reset_index(drop=True))
    print(f'  有效 Offer 数: {len(p3)}')

    # ==================== Point 4：汇总指标 ====================
    print('Point 4: 汇总 30天 / 最近1天 指标 …')
    fv = flow_df[flow_df['Offer ID'].isin(p3['Offer ID'])]

    # Step1: 过去 30 天
    a30 = (fv.groupby('Offer ID')
           .agg(tc30=('Total Clicks', 'sum'),
                cv30=('Total Conversions', 'sum'),
                rv30=('Total Revenue', 'sum'),
                co30=('Total Cost', 'sum'),
                pf30=('Total Profit', 'sum'))
           .reset_index())
    a30['cr30'] = safe_div(a30['cv30'], a30['tc30'])

    # Step2: 最近 1 天
    fv1 = fv[fv['Time'] == latest_day]
    a1 = (fv1.groupby('Offer ID')
          .agg(tc1=('Total Clicks', 'sum'),
               cv1=('Total Conversions', 'sum'),
               rv1=('Total Revenue', 'sum'),
               co1=('Total Cost', 'sum'),
               pf1=('Total Profit', 'sum'))
          .reset_index())
    a1['cr1'] = safe_div(a1['cv1'], a1['tc1'])

    p4 = a30.merge(a1, on='Offer ID', how='left').fillna(0)
    p4 = p4.rename(columns={
        'tc30': '过去30天的Total Clicks',   'cv30': '过去30天的Total Conversions',
        'cr30': '过去30天的CR',             'rv30': '过去30天的Total Revenue',
        'co30': '过去30天的Total Cost',     'pf30': '过去30天的Total Profit',
        'tc1':  '最近1天的Total Clicks',    'cv1':  '最近1天的Total Conversions',
        'cr1':  '最近1天的CR',              'rv1':  '最近1天的Total Revenue',
        'co1':  '最近1天的Total Cost',      'pf1':  '最近1天的Total Profit',
    })

    # Step3: 预算剩余空间
    p4 = p4.merge(p3[['Offer ID', 'Total Caps']], on='Offer ID', how='left')
    p4['预算剩余空间'] = p4['Total Caps'] - p4['最近1天的Total Conversions']
    p4 = p4.drop(columns=['Total Caps'], errors='ignore')  # 避免与 p3 合并时产生 Total Caps_x/y，最终由 p3 保留 Total Caps

    # ==================== Point 5：事件统计 ====================
    print('Point 5: 事件统计 …')
    ev = event_df[event_df['Offer ID'].isin(p3['Offer ID'])]

    # Step1: reject（前一天）
    rej5 = (ev[(ev['Event'] == 'reject') & (ev['Time'] == prev_day)]
            .groupby('Offer ID').size().reset_index(name='event_num'))
    rej5['Time'], rej5['event'] = prev_day, 'reject'

    # Step2: 非 reject（最近一天）
    oth5 = (ev[(ev['Event'] != 'reject') & (ev['Time'] == latest_day)]
            .groupby(['Offer ID', 'Event']).size().reset_index(name='event_num'))
    oth5['Time'] = latest_day
    oth5 = oth5.rename(columns={'Event': 'event'})

    p5 = pd.concat([rej5, oth5], ignore_index=True)

    # ==================== Point 6：Offer 维度 reject / event rate ====================
    print('Point 6: Offer 维度 reject / event rate …')
    conv_day = (flow_df.groupby(['Offer ID', 'Time'])['Total Conversions']
                .sum().reset_index())

    # Step2: reject
    p6r = p5[p5['event'] == 'reject'].merge(conv_day, on=['Offer ID', 'Time'], how='inner')
    p6r['前一天的reject rate'] = safe_div(
        p6r['event_num'], p6r['event_num'] + p6r['Total Conversions'])
    p6r = p6r.rename(columns={'event_num': '前一天的reject num'})
    p6r = p6r[['Offer ID', '前一天的reject num', '前一天的reject rate']]

    # Step3: 非 reject
    p6o = p5[p5['event'] != 'reject'].merge(conv_day, on=['Offer ID', 'Time'], how='inner')
    p6o['event_rate'] = safe_div(p6o['event_num'], p6o['Total Conversions'])
    if len(p6o):
        p6e = agg_event_str(p6o, 'Offer ID', '最近一天的event事件数和事件率')
    else:
        p6e = pd.DataFrame(columns=['Offer ID', '最近一天的event事件数和事件率'])

    p6 = p6r.merge(p6e, on='Offer ID', how='outer')

    # ==================== Point 7：广告主维度 reject ====================
    print('Point 7: 广告主维度 reject …')
    rej_adv = (event_df[(event_df['Event'] == 'reject') & (event_df['Time'] == prev_day)]
               .groupby('Advertiser').size()
               .reset_index(name='前一天广告主总的reject num'))

    # Step2: 广告主 conversions（前一天）：flow_df 前一天整体按 Advertiser 汇总
    conv_adv = (flow_df[flow_df['Time'] == prev_day]
                .groupby('Advertiser')['Total Conversions'].sum().reset_index())

    # Step3: 合并
    p7 = conv_adv.merge(rej_adv, on='Advertiser', how='left')
    p7['前一天广告主总的reject num'] = p7['前一天广告主总的reject num'].fillna(0)
    p7['前一天广告主总的reject rate'] = safe_div(
        p7['前一天广告主总的reject num'],
        p7['前一天广告主总的reject num'] + p7['Total Conversions'])
    p7 = p7[['Advertiser', '前一天广告主总的reject num', '前一天广告主总的reject rate']]

    # ==================== Point 8：合并 Offer 维度 ====================
    print('Point 8: 合并 Offer 维度 …')
    p8 = (p3
          .merge(p4, on='Offer ID', how='left')
          .merge(p7, on='Advertiser', how='left')
          .merge(p6, on='Offer ID', how='left'))
    for c in ('前一天广告主总的reject num', '前一天广告主总的reject rate',
              '前一天的reject num', '前一天的reject rate'):
        if c in p8.columns:
            p8[c] = p8[c].fillna(0)

    # ==================== Point 9：不在预算跟进表的 Offer ====================
    print('Point 9: 不在预算跟进表的 Offer …')

    # Step1: 基础筛选
    payin_num = p8['Payin'].apply(extract_payin_num)
    daily_rev = (flow_df.groupby(['Offer ID', 'Time'])['Total Revenue']
                 .sum().reset_index())
    #ids_rev10 = set(daily_rev[daily_rev['Total Revenue'] >= 10]['Offer ID'])
    p9_mask = (payin_num >= 0.12) & (p8['Status'] == 'ACTIVE') 
    p9b = p8[p9_mask].copy().reset_index(drop=True)

    # Step2: 排除已在预算跟进表的
    budget_ids = set(budget_df['Offer ID'].dropna()) if 'Offer ID' in budget_df.columns else set()
    p9b = p9b[~p9b['Offer ID'].isin(budget_ids)].reset_index(drop=True)
    print(f'  Point9 候选 Offer 数: {len(p9b)}')

    p9_result = pd.DataFrame()
    if len(p9b):
        # Step3: 下游 Affiliate 最近一天流水
        fl1 = flow_df[flow_df['Time'] == latest_day]
        p9a = p9b[['Offer ID']].merge(
            fl1[['Offer ID', 'Affiliate', 'Total Clicks', 'Total Conversions',
                 'Total Revenue', 'Total Cost', 'Total Profit']],
            on='Offer ID', how='inner')
        p9a['CR'] = safe_div(p9a['Total Conversions'], p9a['Total Clicks'])
        p9a = p9a.rename(columns={
            'Total Clicks':      '下游Affiliate最近一天的Total Clicks',
            'Total Conversions': '下游Affiliate最近一天的Total Conversions',
            'CR':                '下游Affiliate最近一天的CR',
            'Total Revenue':     '下游Affiliate最近一天的Total Revenue',
            'Total Cost':        '下游Affiliate最近一天的Total Cost',
            'Total Profit':      '下游Affiliate最近一天的Total Profit',
        })

        # Step4 + Step5 + Step6: reject / event
        p9a = calc_affiliate_events(
            p9a, event_df, prev_day, latest_day,
            '下游Affiliate最近一天的Total Conversions')

        p9d = p9b.merge(p9a, on='Offer ID', how='left')

        # Step7: 预算跟进状态 + 待办事项
        p9d['预算跟进状态'] = '待更新'

        def _p9_todo(r):
            rr = r.get('下游Affiliate前一天的reject_rate', 0) or 0
            oe = r.get('最近一天的event事件数和事件率')
            ae = r.get('下游Affiliate最近一天的事件数和事件率')
            has_oe = pd.notna(oe) and str(oe).strip() != ''
            has_ae = pd.notna(ae) and str(ae).strip() != ''
            if rr >= 0.1:
                return '下游reject>=10%，优先优化reject'
            if has_oe and not has_ae:
                return '下游没有事件率，先降低推量优先级'
            return '重点push该下游'

        p9d['待办事项'] = p9d.apply(_p9_todo, axis=1)

        # Step8: 取出预算跟进表含「Affiliate」的列名并去标记 → 左连接 Step3 的 Affiliate，匹配不到的列名组合成一行，下游指标为空
        aff_hdr = [c for c in budget_df.columns if 'Affiliate' in str(c) and c != 'Affiliate']
        known_affs = [re.sub(r'^Affiliate\s*', '', c).strip() for c in aff_hdr]
        empty_downstream = [
            '下游Affiliate最近一天的Total Clicks', '下游Affiliate最近一天的Total Conversions', '下游Affiliate最近一天的CR',
            '下游Affiliate最近一天的Total Revenue', '下游Affiliate最近一天的Total Cost', '下游Affiliate最近一天的Total Profit',
            '下游Affiliate前一天的reject_num', '下游Affiliate前一天的reject_rate', '下游Affiliate最近一天的事件数和事件率',
        ]
        extra = []
        for oid in p9b['Offer ID'].unique():
            exist = set(p9d.loc[p9d['Offer ID'] == oid, 'Affiliate'].dropna())
            missing = [a for a in known_affs if a not in exist]
            if missing:
                base = p9b[p9b['Offer ID'] == oid].iloc[0].to_dict()
                base['Affiliate'] = ', '.join(missing)
                base['预算跟进状态'] = '待更新'
                base['待办事项'] = f'剩余这些Affiliate未产生流水，可关注是否要推预算:{", ".join(missing)}'
                for k in empty_downstream:
                    if k in p9d.columns:
                        base[k] = np.nan
                extra.append(base)
        if extra:
            p9d = pd.concat([p9d, pd.DataFrame(extra)], ignore_index=True)
        # 删除 Affiliate 为空的行
        p9d = p9d[p9d['Affiliate'].notna() & (p9d['Affiliate'].astype(str).str.strip() != '')]
        p9_result = p9d

    # ==================== Point 10：已在预算跟进表的 Offer ====================
    print('Point 10: 已在预算跟进表的 Offer …')
    aff_hdr = [c for c in budget_df.columns if 'Affiliate' in str(c) and c != 'Affiliate']
    p10_result = pd.DataFrame()

    if aff_hdr and 'Offer ID' in budget_df.columns:
        # Step1: 转置 Affiliate 列
        rows = []
        for _, r in budget_df.iterrows():
            oid = r['Offer ID']
            for col in aff_hdr:
                aff_name = re.sub(r'^Affiliate\s*', '', col).strip()
                rows.append({'Offer ID': oid, 'Affiliate': aff_name, '预算跟进状态': r[col]})
        p10_piv = pd.DataFrame(rows)

        # Step2: 内连接 Point8
        p10b = p10_piv.merge(p8, on='Offer ID', how='inner')
        print(f'  Point10 候选行数: {len(p10b)}')

        if len(p10b):
            push_set = {'能跑出', '没上量', '已下发'}

            # Step3: 下游 Affiliate 最近一天流水
            fl1 = flow_df[flow_df['Time'] == latest_day]
            p10a = p10b[['Offer ID', 'Affiliate']].drop_duplicates().merge(
                fl1[['Offer ID', 'Affiliate', 'Total Clicks', 'Total Conversions',
                     'Total Revenue', 'Total Cost', 'Total Profit']],
                on=['Offer ID', 'Affiliate'], how='left')
            p10a['CR'] = safe_div(
                p10a['Total Conversions'].fillna(0), p10a['Total Clicks'].fillna(0))
            p10a = p10a.rename(columns={
                'Total Clicks':      '下游Affiliate最近一天的Total Clicks',
                'Total Conversions': '下游Affiliate最近一天的Total Conversions',
                'CR':                '下游Affiliate最近一天的CR',
                'Total Revenue':     '下游Affiliate最近一天的Total Revenue',
                'Total Cost':        '下游Affiliate最近一天的Total Cost',
                'Total Profit':      '下游Affiliate最近一天的Total Profit',
            })

            # 只保留：最近一天 Total Clicks > 0 或 预算跟进状态 in push_set，再进行 Step4–Step7
            p10a = p10a.merge(
                p10b[['Offer ID', 'Affiliate', '预算跟进状态']].drop_duplicates(),
                on=['Offer ID', 'Affiliate'], how='left')
            tc = p10a['下游Affiliate最近一天的Total Clicks'].fillna(0)
            st = p10a['预算跟进状态'].astype(str).str.strip()
            p10a = p10a[(tc > 0) | st.isin(push_set)].copy()
            p10a = p10a.drop(columns=['预算跟进状态'], errors='ignore')

            # Step4 + Step5 + Step6
            p10a = calc_affiliate_events(
                p10a, event_df, prev_day, latest_day,
                '下游Affiliate最近一天的Total Conversions')

            p10d = p10b.merge(p10a, on=['Offer ID', 'Affiliate'], how='inner')

            # Step7: 待办事项

            def _p10_todo(r):
                if str(r.get('Status', '')).strip().upper() == 'PAUSE':
                    return '该预算已暂停，暂时无需关注'
                st = str(r.get('预算跟进状态', ''))
                pushed = st in push_set
                rr  = r.get('下游Affiliate前一天的reject_rate', 0) or 0
                rev = r.get('下游Affiliate最近一天的Total Revenue')
                rev = 0 if pd.isna(rev) else rev
                oe  = r.get('最近一天的event事件数和事件率')
                ae  = r.get('下游Affiliate最近一天的事件数和事件率')
                has_oe = pd.notna(oe) and str(oe).strip() != ''
                has_ae = pd.notna(ae) and str(ae).strip() != ''

                if rr >= 0.1 and pushed:
                    return '该下游为已push下游，但下游reject>=10%，优先优化reject'
                if rr >= 0.1 and not pushed:
                    return '该下游不是已push下游，且下游reject>=10%，优先优化reject'
                if rev == 0 and pushed:
                    return '该下游已push，但未产生流水，需要继续push'
                if has_oe and not has_ae and pushed:
                    return '该下游为已push下游，但下游没有事件率，先降低推量优先级'
                if has_oe and not has_ae and not pushed:
                    return '该下游不是已push下游，且下游没有事件率，先降低推量优先级'
                if pushed:
                    return '该下游为已push下游，目前已经跑出量级，继续沟通是否可以加量'
                return '该下游不是已push下游，目前已经跑出量级，明确是否要继续推同时更新预算状态'

            p10d['待办事项'] = p10d.apply(_p10_todo, axis=1)
            p10_result = p10d

    # ==================== Point 11：合并最终输出 ====================
    print('Point 11: 合并输出 …')
    final_cols = [
        'Adv Offer ID', 'Offer ID', 'Status', 'GEO', 'Payin', 'Total Caps',
        'Advertiser', 'App ID',
        '过去30天的Total Clicks', '过去30天的Total Conversions', '过去30天的CR',
        '过去30天的Total Revenue', '过去30天的Total Cost', '过去30天的Total Profit',
        '最近1天的Total Clicks', '最近1天的Total Conversions', '最近1天的CR',
        '最近1天的Total Revenue', '最近1天的Total Cost', '最近1天的Total Profit',
        '预算剩余空间',
        '前一天的reject num', '前一天的reject rate',
        '最近一天的event事件数和事件率',
        '前一天广告主总的reject num', '前一天广告主总的reject rate',
        'Affiliate',
        '下游Affiliate最近一天的Total Clicks', '下游Affiliate最近一天的Total Conversions',
        '下游Affiliate最近一天的CR', '下游Affiliate最近一天的Total Revenue',
        '下游Affiliate最近一天的Total Cost', '下游Affiliate最近一天的Total Profit',
        '下游Affiliate前一天的reject_num', '下游Affiliate前一天的reject_rate',
        '下游Affiliate最近一天的事件数和事件率',
        '预算跟进状态', '待办事项',
    ]

    result = pd.concat([p9_result, p10_result], ignore_index=True)
    for c in final_cols:
        if c not in result.columns:
            result[c] = np.nan
    result = result[final_cols]

    result.to_excel(output_path, index=False, engine='openpyxl')
    print(f'\n✅ 完成！共 {len(result)} 条记录 → {output_path}')
    return result


if __name__ == '__main__':
    # ── 可调试模式：设为 True 并在下方填写路径，直接运行即可（不依赖命令行参数）──
    DEBUG = False
    DEBUG_EXCEL_PATH = r'/Users/doraemonfang/Desktop/你的文件.xlsx'   # 调试时填入实际路径
    DEBUG_OUTPUT_PATH = None  # 留空则自动生成；也可指定如 r'C:\out\结果.xlsx'

    if DEBUG and DEBUG_EXCEL_PATH:
        main(DEBUG_EXCEL_PATH, DEBUG_OUTPUT_PATH)
    elif len(sys.argv) >= 2:
        main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None)
    else:
        excel_path = input('请输入 Excel 文件路径: ').strip().strip('"\'')
        if not excel_path:
            print('未输入路径，已退出。')
            sys.exit(1)
        out_path = input('请输入输出路径（直接回车则自动生成）: ').strip().strip('"\'') or None
        main(excel_path, out_path)
