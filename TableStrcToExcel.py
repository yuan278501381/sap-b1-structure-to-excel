import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse

# ================= 配置区域 =================
# 请根据实际情况修改数据库连接信息
DB_SERVER = '192.168.134.9'  # SQLSERVER 地址
DB_NAME = 'KCC_test'  # 【请确认】您的数据库名称（例如账套名或 SBO_COMMON）
DB_USER = 'sa'  # 数据库用户名
DB_PASSWORD = '123456@a'  # 数据库密码

# 输出文件名
OUTPUT_FILE = 'SAP_UDO_字段结构.xlsx'

# 您的 SQL 查询语句 (已针对中文乱码进行优化，添加了 N 前缀)
SQL_QUERY = """
SELECT 
      t2.Code  UDO代码,
      t2.Name  UDO名,
      t2.Type UDO类型,
      T0.TableID 表, 
      t3.Descr 表名称,

      T0.AliasID AS 字段名, 
      T0.Descr AS [描述],

      CASE 
        WHEN T0.TypeID = 'A' AND T0.EditType = '' THEN N'字母数字-定期'
        WHEN T0.TypeID = 'A' AND T0.EditType = 'T' THEN N'文本'
        WHEN T0.TypeID = 'A' AND T0.EditType = 'I' THEN N'图片'
        WHEN T0.TypeID = 'A' AND T0.EditType = 'C' THEN N'复选框'
        WHEN T0.TypeID = 'N' AND T0.EditType = 'T' THEN N'时间'
        WHEN T0.TypeID = 'N' THEN N'数字'
        WHEN T0.TypeID = 'D' THEN N'日期'
        WHEN T0.TypeID = 'B' AND T0.EditType = '%' THEN N'单位与总计-百分比'
        WHEN T0.TypeID = 'B' AND T0.EditType = 'Q' THEN N'单位与总计-数量'
        WHEN T0.TypeID = 'B' AND T0.EditType = 'P' THEN N'单位与总计-价格'
        WHEN T0.TypeID = 'B' AND T0.EditType = 'S' THEN N'单位与总计-金额'
        WHEN T0.TypeID = 'B' AND T0.EditType = 'M' THEN N'单位与总计-汇率'
        WHEN T0.TypeID = 'M' THEN N'备注'
        ELSE N'其他/未知 (' + CAST(T0.TypeID AS NVARCHAR(10)) + N'_' + CAST(T0.EditType AS NVARCHAR(10)) + N')' 
      END AS [类型],

      CASE WHEN T0.TypeID = 'B' THEN NULL ELSE T0.EditSize END AS 长度,

      -- 可选值 (Valid Values)
      (SELECT STRING_AGG(CAST(T1.FldValue AS NVARCHAR(MAX)) + N':' + CAST(T1.Descr AS NVARCHAR(MAX)) + N';', CHAR(13) + CHAR(10)) 
       FROM UFD1 T1 
       WHERE T1.TableID = T0.TableID AND T1.FieldID = T0.FieldID) AS 可选值,        
      T0.Dflt 默认值, 
      T0.RTable 链接表

FROM CUFD T0
LEFT JOIN (
    -- 1. 获取 UDO 主表关联
    SELECT Code, Name, concat('@', TableName )TableName, Case when TYPE =1 then N'主数据' when TYPE=3 then N'单据'END Type
    FROM OUDO
    UNION 
    SELECT t.Code, t.Name, concat('@', t1.TableName )TableName, Case when TYPE =1 then N'主数据' when TYPE=3 then N'单据'END Type
    FROM OUDO t
    INNER JOIN UDO1 t1 ON t.Code = t1.Code
) t2 ON T0.TableID = t2.TableName

LEFT JOIN OUTB t3 on CONCAT('@', t3.TableName)= t0.TableID
WHERE 
      1 = 1 
      and t0.TableID in (
   '@CH_ORGITYPE','@ITEMTYPE','@KCC_ATTR1','@KCC_PROD_LINE','@MAIN_COLOR_CARD','@MAIN_COLOR_CODE','@PRODUCT_ATTR','@PRODUCT_ATTR_1','@RULE_EXPR','@RULE_EXPR_1','@RULE_GEN','@RULE_GEN_1','@RULE_ITEM_MASTER','@RULE_ITEM_MASTER_1','@RULE_ITMCODE_CRT','@RULE_ITMCODE_CRT_1','@RULE_ITMCODE_CRT_2','@RULE_TXT_EXPR','@RULE_TXT_EXPR_1','@TMP_CLASS','@TMP_LOWERSTOP','@TMP_STRAPE','@TMP_TEETH','@TMP_UPPERSTOP','@TYPE','@WHS_HEAD','@ZIPPER_HEAD_COLOR','@ZIPPER_HEAD_COLORCD','@ZIPPER_HEAD_CONNCT','@ZIPPER_HEAD_LABEL','@ZIPPER_PULL_TAB','@ZIPPER_PULL_TAB_1'
      )

ORDER BY t2.Code desc , T0.TableID, T0.FieldID;
"""


def clean_sheet_name(name):
    """Excel Sheet 名称不能包含特殊字符，且长度不能超过31个字符"""
    if not name:
        return "Unknown"
    # 将 None 转换为空字符串
    name = str(name)
    invalid_chars = ['\\', '/', '*', '[', ']', ':', '?']
    for char in invalid_chars:
        name = name.replace(char, '_')
    # Excel 限制 31 个字符（Python 3 中中文算 1 个字符，这里截取 30 个保底）
    return name[:30]


def export_to_excel():
    print("正在连接数据库...")

    # 构建连接字符串
    # ODBC Driver 17 支持 Unicode，通常无需额外 charset 设置
    # Python 3 默认也是 UTF-8，结合 N 前缀可以确保中文正常
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASSWORD}"
    )

    # 建立引擎
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    try:
        # 1. 读取数据
        print("正在执行 SQL 查询...")
        df = pd.read_sql(SQL_QUERY, engine)

        print(f"查询完成，共获取 {len(df)} 行数据。正在处理 Excel...")

        # 2. 创建 Excel Writer
        # 使用 xlsxwriter 引擎，它对 Unicode 支持极好，不会出现 CSV 那种需要 BOM 头的问题
        with pd.ExcelWriter(OUTPUT_FILE, engine='xlsxwriter') as writer:

            # --- 处理 UDO 部分 ---
            df_udo = df[df['UDO代码'].notna()]
            udos = df_udo['UDO代码'].unique()

            for udo_code in udos:
                udo_data = df_udo[df_udo['UDO代码'] == udo_code]

                # 使用 UDO代码作为 Sheet 名
                sheet_name = clean_sheet_name(f"UDO_{udo_code}")

                print(f"正在写入 Sheet: {sheet_name} (行数: {len(udo_data)})")
                udo_data.to_excel(writer, sheet_name=sheet_name, index=False)

            # --- 处理 非UDO (独立表) 部分 ---
            df_no_udo = df[df['UDO代码'].isna()]
            tables = df_no_udo['表'].unique()

            for table_id in tables:
                table_data = df_no_udo[df_no_udo['表'] == table_id]

                # 清理表名作为 Sheet 名
                clean_table = table_id.replace('@', '')
                sheet_name = clean_sheet_name(f"表_{clean_table}")

                print(f"正在写入 Sheet: {sheet_name} (行数: {len(table_data)})")
                table_data.to_excel(writer, sheet_name=sheet_name, index=False)

            # 如果没有数据
            if len(df) == 0:
                print("警告: 查询结果为空，生成了空文件。")
                pd.DataFrame().to_excel(writer, sheet_name="无数据")

        print(f"\n成功! 文件已保存为: {OUTPUT_FILE}")
        print("提示: 这是一个 Excel 文件，原生支持中文，无需担心乱码。")

    except Exception as e:
        print(f"\n发生错误: {e}")
        print("提示: 请确保已安装 pandas, sqlalchemy, pyodbc 和 xlsxwriter 库。")
        print("pip install pandas sqlalchemy pyodbc xlsxwriter")


if __name__ == "__main__":
    export_to_excel()