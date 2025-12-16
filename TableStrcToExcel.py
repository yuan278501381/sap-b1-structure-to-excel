import pandas as pd
from sqlalchemy import create_engine
import urllib.parse
import pyodbc
import socket

# ================= 配置区域 =================
DB_SERVER = '192.168.134.9'
DB_NAME = 'KCC_test'
DB_USER = 'sa'
DB_PASSWORD = '123456@a'

OUTPUT_FILE = 'SAP_UDO_字段结构.xlsx'

# SQL 查询保持不变
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

      (SELECT STRING_AGG(CAST(T1.FldValue AS NVARCHAR(MAX)) + N':' + CAST(T1.Descr AS NVARCHAR(MAX)) + N';', CHAR(13) + CHAR(10)) 
       FROM UFD1 T1 
       WHERE T1.TableID = T0.TableID AND T1.FieldID = T0.FieldID) AS 可选值,        
      T0.Dflt 默认值, 
      T0.RTable 链接表

FROM CUFD T0
LEFT JOIN (
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


def clean_sheet_name(name, used_names):
    """Excel Sheet 名称清洗并确保唯一"""
    if pd.isna(name) or name == "":
        name = "Unknown"
    name = str(name)
    invalid_chars = ['\\', '/', '*', '[', ']', ':', '?']
    for char in invalid_chars:
        name = name.replace(char, '_')

    # 截取前30个字符
    base_name = name[:25]  # 留一点空间给后缀
    candidate = base_name
    counter = 1

    while candidate in used_names:
        candidate = f"{base_name}_{counter}"
        counter += 1

    used_names.add(candidate)
    return candidate


def get_best_driver():
    """检测并返回最佳的 SQL Server ODBC 驱动"""
    try:
        installed_drivers = pyodbc.drivers()
        print(f"系统中已安装的 ODBC 驱动: {installed_drivers}")
    except Exception:
        return None

    preferences = [
        'ODBC Driver 18 for SQL Server',
        'ODBC Driver 17 for SQL Server',
        'SQL Server Native Client 11.0',
        'ODBC Driver 13 for SQL Server',
        'SQL Server'
    ]

    for pref in preferences:
        if pref in installed_drivers:
            return pref

    for driver in installed_drivers:
        if 'SQL Server' in driver:
            return driver
    return None


def test_tcp_connection(host, port=1433):
    """测试 TCP 端口连通性"""
    print(f"\n--- 开始网络诊断 ---")
    print(f"正在尝试连接主机 {host} 的端口 {port} ...")
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex((host, port))
        sock.close()

        if result == 0:
            print(f"✅ 成功: 网络通畅，端口 {port} 开放。")
            return True
        else:
            print(f"❌ 失败: 无法连接到 {host}:{port} (错误代码: {result})")
            return False
    except Exception as e:
        print(f"❌ 测试出错: {e}")
        return False
    finally:
        print(f"--- 诊断结束 ---\n")


def export_to_excel():
    # 0. 网络自检
    if not test_tcp_connection(DB_SERVER, 1433):
        print("警告: 网络连接测试失败，建议检查防火墙。")

    print("正在连接数据库...")
    driver_name = get_best_driver()
    if not driver_name:
        print("错误: 未找到任何 SQL Server ODBC 驱动！")
        return
    print(f"正在使用驱动程序: {driver_name}")

    # 1. 构建连接字符串 (强制 TCP, 信任证书)
    server_addr = DB_SERVER
    if not server_addr.startswith('tcp:'):
        server_addr = f'tcp:{server_addr}'

    params = urllib.parse.quote_plus(
        f"DRIVER={{{driver_name}}};"
        f"SERVER={server_addr};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASSWORD};"
        f"TrustServerCertificate=yes;"
    )

    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    try:
        print("正在执行 SQL 查询...")
        df = pd.read_sql(SQL_QUERY, engine)
        print(f"查询完成，共获取 {len(df)} 行数据。正在生成 Excel...")

        # 定义要显示在表格里的核心列（去掉重复的表名、UDO名等）
        display_columns = ['字段名', '描述', '类型', '长度', '可选值', '默认值', '链接表']

        with pd.ExcelWriter(OUTPUT_FILE, engine='xlsxwriter') as writer:
            workbook = writer.book

            # 定义格式
            header_fmt = workbook.add_format({
                'bold': True, 'text_wrap': True, 'valign': 'top', 'fg_color': '#D7E4BC', 'border': 1})
            title_fmt = workbook.add_format({
                'bold': True, 'font_size': 12, 'font_color': '#366092', 'bg_color': '#F2F2F2', 'border': 1})

            used_sheet_names = set()

            # --- 1. 处理 UDO (按 UDO代码 分组) ---
            df_udo = df[df['UDO代码'].notna()]

            # 按 UDO代码 分组迭代
            for udo_code, udo_group in df_udo.groupby('UDO代码'):
                # 取第一行的 UDO名 作为 Sheet 名
                raw_sheet_name = udo_group.iloc[0]['UDO名']
                if pd.isna(raw_sheet_name):
                    raw_sheet_name = udo_code

                sheet_name = clean_sheet_name(raw_sheet_name, used_sheet_names)
                print(f"处理 Sheet: {sheet_name} (UDO: {udo_code})")

                # 初始化起始行
                current_row = 0

                # 找出该 UDO 下包含的所有表 (通常有主表和子表)
                # 按照 SQL 排序，通常主表在前
                table_list = udo_group['表'].unique()

                for table_id in table_list:
                    # 获取该表的数据
                    table_data = udo_group[udo_group['表'] == table_id]
                    table_desc = table_data.iloc[0]['表名称']
                    if pd.isna(table_desc): table_desc = ""

                    # 1. 写入小标题 (表名)
                    # 先写入一个空的 DataFrame 来激活 sheet（如果还没创建）
                    if current_row == 0:
                        pd.DataFrame().to_excel(writer, sheet_name=sheet_name, startrow=0)

                    worksheet = writer.sheets[sheet_name]

                    # 写入类似 "Table: @HEAD - 描述" 的标题
                    title_text = f"表: {table_id}  {table_desc}"
                    worksheet.merge_range(current_row, 0, current_row, len(display_columns) - 1, title_text, title_fmt)
                    current_row += 1

                    # 2. 写入数据表格
                    # 仅写入需要的列
                    data_to_write = table_data[display_columns]
                    data_to_write.to_excel(writer, sheet_name=sheet_name, startrow=current_row, index=False)

                    # 设置表头格式 (to_excel 默认格式比较简陋，我们可以覆盖)
                    for col_num, value in enumerate(data_to_write.columns.values):
                        worksheet.write(current_row, col_num, value, header_fmt)

                    # 3. 更新行号 (数据行数 + 表头1行 + 空行间隔2行)
                    current_row += len(data_to_write) + 1 + 2

            # --- 2. 处理 非UDO (独立表) ---
            df_no_udo = df[df['UDO代码'].isna()]

            # 按 表ID 分组迭代
            for table_id, table_group in df_no_udo.groupby('表'):
                # 取 表名称 作为 Sheet 名
                raw_sheet_name = table_group.iloc[0]['表名称']
                if pd.isna(raw_sheet_name):
                    raw_sheet_name = table_id.replace('@', '')

                sheet_name = clean_sheet_name(raw_sheet_name, used_sheet_names)
                print(f"处理 Sheet: {sheet_name} (Table: {table_id})")

                # 直接写入数据
                data_to_write = table_group[display_columns]
                data_to_write.to_excel(writer, sheet_name=sheet_name, index=False)

                # 美化一下表头
                worksheet = writer.sheets[sheet_name]
                for col_num, value in enumerate(data_to_write.columns.values):
                    worksheet.write(0, col_num, value, header_fmt)

                # 自动调整列宽 (简单的估算)
                worksheet.set_column(0, 0, 20)  # 字段名
                worksheet.set_column(1, 1, 40)  # 描述
                worksheet.set_column(4, 4, 30)  # 可选值

        print(f"\n成功! 文件已保存为: {OUTPUT_FILE}")

    except Exception as e:
        print(f"\n发生错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    export_to_excel()