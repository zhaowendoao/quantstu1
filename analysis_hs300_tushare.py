import tushare as ts
import pandas as pd
import numpy as np
import akshare as ak
import time
import os

# ===================== Tushare Token 配置 =====================
# 请在此处填入你的 Tushare Token（注册后获取：https://tushare.pro/）
TUSHARE_TOKEN = "60384c84a271ef467b537f8994a8cedd65739ede48eec2e42ede6372"  # ← 替换为你的 Token
pro = ts.pro_api(TUSHARE_TOKEN)

# ===================== 1. 基础配置 =====================
print("=" * 60)
print("沪深300因子研究数据处理 - Tushare版")
print("=" * 60)
"""
# 获取沪深300最新成分股（Tushare接口）
# index_weight: 沪深300指数成分股权重
hs300_weight = pro.index_weight(index_code='399300.SZ', start_date='20240101')
# 获取最新一期的成分股代码
latest_date = hs300_weight['trade_date'].max()
hs300_cons = hs300_weight[hs300_weight['trade_date'] == latest_date]['con_code'].tolist()

# 补全6位代码并去重，取前10只
stock_code_list = hs300_cons[:10]
# 转换格式：000001.SZ -> 000001
stock_code_list = [code.split('.')[0] for code in stock_code_list]
"""
# 获取沪深300最新成分股
hs300_stocks = ak.index_stock_cons(symbol="000300")
# 补全6位代码并去重，取前10只（修复：[:1] -> [:10]）
stock_code_list = hs300_stocks["品种代码"].astype(str).str.zfill(6).unique().tolist()[:2]
print(f"选取的沪深300成分股（10只）：{stock_code_list}")
print(f"选取股票数量：{len(stock_code_list)}")

# 创建所需目录
os.makedirs("raw_data", exist_ok=True)
os.makedirs("clean_data", exist_ok=True)
os.makedirs("final_data", exist_ok=True)

# ===================== 2. 批量获取日线行情数据（Tushare版） =====================
print("\n" + "=" * 60)
print("第2步：获取日线行情数据")
print("=" * 60)

raw_price_list = []

for code in stock_code_list:
    try:
        # Tushare接口：daily（日线行情）+ adj_factor（复权因子）
        # 获取日线数据
        df = pro.daily(
            ts_code=code + '.SZ' if code.startswith(('0', '3')) else code + '.SH',
            start_date='20240101',
            end_date='20250101'
        )
        
        if df.empty:
            print(f"⚠ 警告：{code} 无行情数据，跳过")
            time.sleep(0.3)
            continue
        
        # 获取复权因子
        adj_df = pro.adj_factor(
            ts_code=code + '.SZ' if code.startswith(('0', '3')) else code + '.SH',
            start_date='20240101',
            end_date='20250101'
        )
        
        # 合并复权因子
        if not adj_df.empty:
            df = df.merge(adj_df[['trade_date', 'adj_factor']], on='trade_date', how='left')
            # 计算前复权价格
            df['open'] = df['open'] * df['adj_factor'] / df['adj_factor'].iloc[-1]
            df['close'] = df['close'] * df['adj_factor'] / df['adj_factor'].iloc[-1]
            df['high'] = df['high'] * df['adj_factor'] / df['adj_factor'].iloc[-1]
            df['low'] = df['low'] * df['adj_factor'] / df['adj_factor'].iloc[-1]
        
        # 重命名字段以保持与原代码一致
        df = df.rename(columns={
            'trade_date': '日期',
            'open': '开盘',
            'close': '收盘',
            'high': '最高',
            'low': '最低',
            'vol': '成交量',
            'amount': '成交额',
            'pct_chg': '涨跌幅'
        })
        df['日期'] = pd.to_datetime(df['日期'])
        df['股票代码'] = code
        df['换手率'] = df.get('turnover_rate', 0)  # Tushare daily 不含换手率，需要单独获取
        
        # 获取换手率数据
        try:
            turnover_df = pro.daily_basic(
                ts_code=code + '.SZ' if code.startswith(('0', '3')) else code + '.SH',
                start_date='20240101',
                end_date='20250101',
                fields='trade_date,turnover_rate'
            )
            if not turnover_df.empty:
                turnover_df = turnover_df.rename(columns={'trade_date': '日期'})
                turnover_df['日期'] = pd.to_datetime(turnover_df['日期'])
                df = df.merge(turnover_df, on='日期', how='left')
        except:
            pass
        
        raw_price_list.append(df)
        print(f"✓ 已获取 {code} 行情数据，共 {len(df)} 条")
        time.sleep(0.3)
        
    except Exception as e:
        print(f"✗ 获取 {code} 行情数据失败：{e}")
        time.sleep(1)

# 合并并保存原始行情数据
if raw_price_list:
    raw_price_df = pd.concat(raw_price_list, ignore_index=True)
    raw_price_df.to_csv("raw_data/raw_行情数据.csv", index=False, encoding="utf-8-sig")
    print(f"\n原始行情数据量：{raw_price_df.shape}")
else:
    print("警告：未获取到任何行情数据！")
    raw_price_df = pd.DataFrame()

# ===================== 3. 获取财务数据（Tushare版） =====================
print("\n" + "=" * 60)
print("第3步：获取财务数据")
print("=" * 60)

raw_finance_list = []

for code in stock_code_list:
    try:
        # Tushare接口：fina_indicator（财务指标数据）
        df = pro.fina_indicator(
            ts_code=code + '.SZ' if code.startswith(('0', '3')) else code + '.SH',
            start_date='20240101',
            end_date='20250101',
            fields='ts_code,ann_date,end_date,roe,netprofit_yoy,revenue_yoy,basic_eps'
        )
        
        if df.empty:
            print(f"⚠ 警告：{code} 无财务数据，跳过")
            time.sleep(0.3)
            continue
        
        # 重命名字段以保持与原代码一致
        df = df.rename(columns={
            'end_date': '报告期',
            'ann_date': '发布日期',
            'roe': '净资产收益率ROE',
            'netprofit_yoy': '净利润同比增长率',
            'revenue_yoy': '营业总收入同比增长率'
        })
        df['报告期'] = pd.to_datetime(df['报告期'])
        df['股票代码'] = code
        
        # 调整列顺序
        cols = ["股票代码", "报告期", "发布日期", "净资产收益率ROE", "净利润同比增长率", "营业总收入同比增长率"]
        cols = [col for col in cols if col in df.columns]
        df = df[cols]
        
        raw_finance_list.append(df)
        print(f"✓ 已获取 {code} 财务数据，共 {len(df)} 条")
        time.sleep(0.3)
        
    except Exception as e:
        print(f"✗ 获取 {code} 财务数据失败：{e}")
        time.sleep(1)
        continue

# 合并财务数据
if raw_finance_list:
    raw_finance_df = pd.concat(raw_finance_list, ignore_index=True)
    
    # 筛选有效数据
    if "股票代码" in raw_finance_df.columns:
        raw_finance_df = raw_finance_df[raw_finance_df["股票代码"].isin(stock_code_list)].copy()
    
    raw_finance_df.to_csv("raw_data/raw_财务数据.csv", index=False, encoding="utf-8-sig")
    print(f"\n原始财务数据量：{raw_finance_df.shape}")
else:
    print("警告：未获取到任何财务数据！")
    raw_finance_df = pd.DataFrame()

# ===================== 4. 清洗行情数据 =====================
print("\n" + "=" * 60)
print("第4步：清洗行情数据")
print("=" * 60)

if not raw_price_df.empty:
    price_df = raw_price_df.copy()
    
    # 4.1 基础格式规整
    price_df["日期"] = pd.to_datetime(price_df["日期"])
    price_df = price_df.set_index(["股票代码", "日期"]).sort_index()
    price_df = price_df[~price_df.index.duplicated(keep="first")]

    # 4.2 无效数据剔除
    price_df = price_df.dropna(subset=["涨跌幅", "成交量"])
    price_df = price_df[price_df["成交量"] > 0]

    # 4.3 缺失值处理
    core_price_cols = ["开盘", "收盘", "最高", "最低", "涨跌幅", "成交量"]
    core_price_cols = [col for col in core_price_cols if col in price_df.columns]
    price_df[core_price_cols] = price_df[core_price_cols].ffill()
    price_df = price_df.dropna(subset=core_price_cols)

    # 4.4 极值缩尾处理
    def winsorize_series(series, min_q=0.01, max_q=0.99):
        lower = series.quantile(min_q)
        upper = series.quantile(max_q)
        return series.clip(lower=lower, upper=upper)

    for col in ["涨跌幅", "成交量", "换手率"]:
        if col in price_df.columns:
            price_df[col] = winsorize_series(price_df[col])

    price_df.to_csv("clean_data/clean_行情数据.csv", encoding="utf-8-sig")
    print(f"✓ 清洗后行情数据量：{price_df.shape}")
else:
    print("✗ 无有效行情数据，跳过清洗")
    price_df = pd.DataFrame()

# ===================== 5. 清洗财务数据 =====================
print("\n" + "=" * 60)
print("第5步：清洗财务数据")
print("=" * 60)

if not raw_finance_df.empty:
    finance_df = raw_finance_df.copy()
    
    if "股票代码" in finance_df.columns:
        finance_df["股票代码"] = finance_df["股票代码"].astype(str).str.zfill(6)
    else:
        print("财务数据无「股票代码」字段，跳过清洗")
        finance_df = pd.DataFrame()

    if not finance_df.empty:
        finance_df["报告期"] = pd.to_datetime(finance_df["报告期"], errors="coerce")
        
        # 锁定核心财务字段
        core_finance_cols = ["股票代码", "报告期", "发布日期", "净资产收益率ROE", 
                            "营业总收入同比增长率", "净利润同比增长率"]
        core_finance_cols = [col for col in core_finance_cols if col in finance_df.columns]
        finance_df = finance_df[core_finance_cols].copy()
        
        # 剔除重复记录
        finance_df = finance_df.drop_duplicates(subset=["股票代码", "报告期"], keep="first")

        # 缺失值处理
        na_cols = ["净资产收益率ROE"]
        na_cols = [col for col in na_cols if col in finance_df.columns]
        if na_cols:
            finance_df = finance_df.dropna(subset=na_cols)

        # 异常值处理
        if "净资产收益率ROE" in finance_df.columns:
            finance_df = finance_df[(finance_df["净资产收益率ROE"] > -100) & (finance_df["净资产收益率ROE"] < 100)]
        if "营业总收入同比增长率" in finance_df.columns:
            finance_df = finance_df[(finance_df["营业总收入同比增长率"] > -500) & (finance_df["营业总收入同比增长率"] < 500)]
        
        finance_df.to_csv("clean_data/clean_财务数据.csv", index=False, encoding="utf-8-sig")
        print(f"✓ 清洗后财务数据量：{finance_df.shape}")
else:
    print("✗ 无有效财务数据，跳过清洗")
    finance_df = pd.DataFrame()

# ===================== 6. 多源数据合并对齐（修复版） =====================
print("\n" + "=" * 60)
print("第6步：多源数据合并对齐")
print("=" * 60)

final_df = pd.DataFrame()

if not price_df.empty and not finance_df.empty:
    # 重置索引便于操作
    clean_price_df = price_df.reset_index()
    clean_finance_df = finance_df.copy()
    
    # 6.1 使用 merge_asof 进行财务数据向前填充
    finance_daily_list = []
    
    for stock_code in stock_code_list:
        # 获取该股票的行情数据
        stock_price = clean_price_df[clean_price_df["股票代码"] == stock_code].copy()
        stock_price = stock_price.sort_values("日期")
        
        # 获取该股票的财务数据
        stock_finance = clean_finance_df[clean_finance_df["股票代码"] == stock_code].copy()
        
        if stock_finance.empty or stock_price.empty:
            continue
        
        # 使用发布日期进行对齐（如果有），否则用报告期
        if "发布日期" in stock_finance.columns:
            stock_finance["对齐日期"] = pd.to_datetime(stock_finance["发布日期"])
        else:
            stock_finance["对齐日期"] = stock_finance["报告期"]
        
        stock_finance = stock_finance.sort_values("对齐日期")
        
        # 提取财务列
        finance_cols = ["对齐日期", "净资产收益率ROE", "营业总收入同比增长率", "净利润同比增长率"]
        finance_cols = [col for col in finance_cols if col in stock_finance.columns]
        stock_finance_subset = stock_finance[finance_cols].copy()
        stock_finance_subset = stock_finance_subset.rename(columns={"对齐日期": "日期"})
        
        # 使用 merge_asof 向前匹配
        merged = pd.merge_asof(
            stock_price,
            stock_finance_subset,
            on="日期",
            direction="backward"
        )
        finance_daily_list.append(merged)
    
    if finance_daily_list:
        finance_daily_df = pd.concat(finance_daily_list, ignore_index=True)
        
        # 6.2 合并行情和财务数据
        price_df_indexed = clean_price_df.set_index(["股票代码", "日期"])
        
        # 提取财务列用于最终合并
        final_finance_cols = ["股票代码", "日期", "净资产收益率ROE", "营业总收入同比增长率", "净利润同比增长率"]
        final_finance_cols = [col for col in final_finance_cols if col in finance_daily_df.columns]
        finance_for_final = finance_daily_df[final_finance_cols].copy()
        finance_for_final = finance_for_final.set_index(["股票代码", "日期"])
        
        # 最终合并
        final_df = pd.merge(
            left=price_df_indexed,
            right=finance_for_final,
            left_index=True,
            right_index=True,
            how="left"
        )
        
        print(f"✓ 对齐后最终数据集维度：{final_df.shape}")
        print(f"  - 有效数据条数：{len(final_df)}")
        if "净资产收益率ROE" in final_df.columns:
            print(f"  - 财务数据覆盖：{final_df['净资产收益率ROE'].notna().sum()} 条")
    else:
        print("✗ 财务数据对齐失败，仅保留行情数据")
        final_df = price_df
        
elif not price_df.empty:
    print("⚠ 无财务数据，仅保留行情数据")
    final_df = price_df
else:
    print("✗ 无有效数据，无法生成最终数据集")

# ===================== 7. 标准化输出 =====================
print("\n" + "=" * 60)
print("第7步：标准化输出")
print("=" * 60)

if not final_df.empty:
    # 字段重命名
    rename_mapping = {
        "开盘": "开盘价",
        "收盘": "收盘价",
        "最高": "最高价",
        "最低": "最低价",
        "净资产收益率ROE": "ROE",
        "营业总收入同比增长率": "营收同比增速",
        "净利润同比增长率": "净利润同比增速"
    }
    rename_mapping = {k: v for k, v in rename_mapping.items() if k in final_df.columns}
    final_standard_df = final_df.rename(columns=rename_mapping)

    # 保留核心字段
    final_cols = ["开盘价", "收盘价", "最高价", "最低价", "涨跌幅", "成交量", "换手率", 
                "ROE", "营收同比增速", "净利润同比增速"]
    final_cols = [col for col in final_cols if col in final_standard_df.columns]
    final_standard_df = final_standard_df[final_cols].copy()

    # 保存最终数据
    final_standard_df.to_csv("final_data/沪深300_投研标准化数据集.csv", encoding="utf-8-sig")
    try:
        final_standard_df.to_parquet("final_data/沪深300_投研标准化数据集.parquet")
    except Exception as e:
        print(f"保存parquet格式失败（需安装pyarrow）：{e}")

    # 输出数据说明文档
    with open("final_data/数据集说明文档.txt", "w", encoding="utf-8") as f:
        f.write("数据集说明：\n")
        f.write("1. 标的范围：沪深300成分股（选取10只）\n")
        f.write("2. 时间周期：2024-01-01 至 2025-01-01 日频数据\n")
        f.write("3. 数据内容：A股日线行情数据+上市公司季度核心财务指标\n")
        f.write("4. 处理流程：原始数据获取→无效值剔除→缺失值填充→极值缩尾→跨频数据对齐→标准化输出\n")
        f.write("5. 适用场景：量化因子测试、策略回测、投研数据分析\n")
        f.write(f"6. 数据来源：Tushare Pro\n")
        f.write(f"7. 生成时间：{pd.Timestamp.now()}\n")
        f.write(f"8. 数据维度：{final_standard_df.shape}\n")
    
    print(f"✓ 最终数据维度：{final_standard_df.shape}")
    print(f"\n数据预览（前10行）：")
    print(final_standard_df.head(10).to)