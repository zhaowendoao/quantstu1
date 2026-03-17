import akshare as ak
import pandas as pd
import numpy as np
import time
import os

# ===================== 1. 基础配置 =====================
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

# ===================== 2. 批量获取日线行情数据 =====================
raw_price_list = []

for code in stock_code_list:
    try:
        # 获取单只股票日线数据（前复权）
        df = ak.stock_zh_a_hist(
            symbol=code, 
            period="daily", 
            start_date="20240101", 
            end_date="20250101", 
            adjust="qfq"
        )
        df["股票代码"] = code
        raw_price_list.append(df)
        print(f"已获取 {code} 行情数据，共 {len(df)} 条")
        time.sleep(1)  # 增加延时，避免反扒
    except Exception as e:
        print(f"获取 {code} 行情数据失败：{e}")
        time.sleep(2)  # 失败后等待更长时间

# 合并并保存原始行情数据
if raw_price_list:
    raw_price_df = pd.concat(raw_price_list, ignore_index=True)
    raw_price_df.to_csv("raw_data/raw_行情数据.csv", index=False, encoding="utf-8-sig")
    print(f"原始行情数据量：{raw_price_df.shape}")
else:
    print("警告：未获取到任何行情数据！")
    raw_price_df = pd.DataFrame()

# ===================== 3. 获取财务数据（修复版） =====================
raw_finance_list = []

for code in stock_code_list:
    try:
        df = ak.stock_financial_abstract_ths(symbol=code, indicator="按报告期")
        
        # 修复：先检查是否为空，为空则跳过
        if df.empty:
            print(f"警告：{code} 无财务数据，跳过")
            time.sleep(1)
            continue  # 关键修复：跳过本次循环
        
        # 补充股票代码列
        df["股票代码"] = code
        df["股票代码"] = df["股票代码"].astype(str).str.zfill(6)
        
        # 调整列顺序
        cols = ["股票代码"] + [col for col in df.columns if col != "股票代码"]
        df = df[cols]
        
        raw_finance_list.append(df)
        print(f"已获取 {code} 财务数据，共 {len(df)} 条")
        time.sleep(1)  # 增加延时
        
    except Exception as e:
        print(f"获取 {code} 财务数据失败：{e}")
        time.sleep(2)

# 合并财务数据
if raw_finance_list:
    raw_finance_df = pd.concat(raw_finance_list, ignore_index=True)
    
    # 筛选有效数据
    if "股票代码" in raw_finance_df.columns:
        raw_finance_df = raw_finance_df[raw_finance_df["股票代码"].isin(stock_code_list)].copy()
    
    raw_finance_df.to_csv("raw_data/raw_财务数据.csv", index=False, encoding="utf-8-sig")
    print(f"原始财务数据量：{raw_finance_df.shape}")
else:
    print("警告：未获取到任何财务数据！")
    raw_finance_df = pd.DataFrame()

# ===================== 4. 清洗行情数据 =====================
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
    price_df[core_price_cols] = price_df[core_price_cols].ffill()
    price_df = price_df.dropna(subset=core_price_cols)

    # 4.4 极值缩尾处理
    def winsorize_series(series, min_q=0.01, max_q=0.99):
        """自定义缩尾函数，处理极端值"""
        lower = series.quantile(min_q)
        upper = series.quantile(max_q)
        return series.clip(lower=lower, upper=upper)

    for col in ["涨跌幅", "成交量", "换手率"]:
        if col in price_df.columns:
            price_df[col] = winsorize_series(price_df[col])

    price_df.to_csv("clean_data/clean_行情数据.csv", encoding="utf-8-sig")
    print(f"清洗后行情数据量：{price_df.shape}，原始行情数据清洗完成")
else:
    print("无有效行情数据，跳过行情数据清洗")
    price_df = pd.DataFrame()

# ===================== 5. 清洗财务数据 =====================
if not raw_finance_df.empty:
    finance_df = raw_finance_df.copy()
    
    # 5.1 基础格式规整
    if "股票代码" in finance_df.columns:
        finance_df["股票代码"] = finance_df["股票代码"].astype(str).str.zfill(6)
    else:
        print("财务数据无「股票代码」字段，跳过财务数据清洗")
        finance_df = pd.DataFrame()

    if not finance_df.empty:
        finance_df["报告期"] = pd.to_datetime(finance_df["报告期"], errors="coerce")
        
        # 锁定核心财务字段（只保留存在的字段）
        core_finance_cols = ["股票代码", "报告期", "净资产收益率ROE", "归属母公司股东的净利润", 
                            "营业总收入同比增长率", "净利润同比增长率"]
        core_finance_cols = [col for col in core_finance_cols if col in finance_df.columns]
        finance_df = finance_df[core_finance_cols].copy()
        
        # 剔除重复记录
        finance_df = finance_df.drop_duplicates(subset=["股票代码", "报告期"], keep="first")

        # 5.2 缺失值处理
        na_cols = ["净资产收益率ROE", "归属母公司股东的净利润"]
        na_cols = [col for col in na_cols if col in finance_df.columns]
        if na_cols:
            finance_df = finance_df.dropna(subset=na_cols)

        # 5.3 异常值处理（修复：放宽阈值）
        if "净资产收益率ROE" in finance_df.columns:
            finance_df = finance_df[(finance_df["净资产收益率ROE"] > -100) & (finance_df["净资产收益率ROE"] < 100)]
        if "营业总收入同比增长率" in finance_df.columns:
            # 修复：原来的阈值(-10, 10)太严格，改为(-500, 500)
            finance_df = finance_df[(finance_df["营业总收入同比增长率"] > -500) & (finance_df["营业总收入同比增长率"] < 500)]
        
        finance_df.to_csv("clean_data/clean_财务数据.csv", index=False, encoding="utf-8-sig")
        print(f"清洗后财务数据量：{finance_df.shape}，财务数据清洗完成")
else:
    print("无有效财务数据，跳过财务数据清洗")
    finance_df = pd.DataFrame()

# ===================== 6. 多源数据合并对齐（修复版） =====================
final_df = pd.DataFrame()

if not price_df.empty and not finance_df.empty:
    # 读取清洗后的数据
    clean_price_df = price_df.reset_index()
    clean_finance_df = finance_df.copy()
    
    # 6.1 财务数据的日频扩展（正确方法：使用 merge_asof 向前填充）
    
    # 准备财务数据：重命名报告期为日期
    finance_for_merge = clean_finance_df.copy()
    finance_for_merge["日期"] = finance_for_merge["报告期"]
    finance_for_merge = finance_for_merge.drop(columns=["报告期"])
    
    # 按股票代码分组进行向前填充
    finance_daily_list = []
    
    for stock_code in stock_code_list:
        # 获取该股票的行情日期
        stock_price = clean_price_df[clean_price_df["股票代码"] == stock_code].copy()
        stock_price = stock_price.sort_values("日期")
        
        # 获取该股票的财务数据
        stock_finance = finance_for_merge[finance_for_merge["股票代码"] == stock_code].copy()
        stock_finance = stock_finance.sort_values("日期")
        
        if stock_finance.empty or stock_price.empty:
            continue
        
        # 使用 merge_asof 进行向前匹配（财务数据填充到后续每一天，直到下一份财报）
        # 这样可以避免"未来函数"问题：只用已经发布的财务数据
        merged = pd.merge_asof(
            stock_price,
            stock_finance,
            on="日期",
            by="股票代码",
            direction="backward",  # 向后查找：用过去的财务数据填充未来日期
            allow_exact_matches=True
        )
        finance_daily_list.append(merged)
    
    if finance_daily_list:
        finance_daily_df = pd.concat(finance_daily_list, ignore_index=True)
        
        # 6.2 合并行情和财务数据
        price_df_indexed = clean_price_df.set_index(["股票代码", "日期"])
        
        # 提取财务列
        finance_cols = ["股票代码", "日期", "净资产收益率ROE", "归属母公司股东的净利润", 
                       "营业总收入同比增长率", "净利润同比增长率"]
        finance_cols = [col for col in finance_cols if col in finance_daily_df.columns]
        finance_for_final = finance_daily_df[finance_cols].copy()
        finance_for_final = finance_for_final.set_index(["股票代码", "日期"])
        
        # 合并
        final_df = pd.merge(
            left=price_df_indexed,
            right=finance_for_final,
            left_index=True,
            right_index=True,
            how="left"
        )
        
        print(f"对齐后最终数据集维度：{final_df.shape}")
    else:
        print("财务数据对齐失败，仅保留行情数据")
        final_df = price_df
        
elif not price_df.empty:
    print("无财务数据，仅保留行情数据")
    final_df = price_df

# ===================== 7. 标准化输出 =====================
if not final_df.empty:
    # 字段重命名
    rename_mapping = {
        "开盘": "开盘价",
        "收盘": "收盘价",
        "最高": "最高价",
        "最低": "最低价",
        "净资产收益率ROE": "ROE",
        "归属母公司股东的净利润": "归母净利润",
        "营业总收入同比增长率": "营收同比增速",
        "净利润同比增长率": "净利润同比增速"
    }
    rename_mapping = {k: v for k, v in rename_mapping.items() if k in final_df.columns}
    final_standard_df = final_df.rename(columns=rename_mapping)

    # 保留核心字段
    final_cols = ["开盘价", "收盘价", "最高价", "最低价", "涨跌幅", "成交量", "换手率", 
                "ROE", "归母净利润", "营收同比增速", "净利润同比增速"]
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
    
    print(f"\n最终数据预览：")
    print(final_standard_df.head(10))
    print(f"\n最终数据统计：")
    print(final_standard_df.describe())

print("\n所有数据处理完成，最终数据已保存至 final_data 目录！")
