import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse
import pyodbc
import socket
import re

# ================= 配置区域 =================
DB_SERVER = '192.168.134.9'
DB_NAME = 'KCC_test'
DB_USER = 'sa'
DB_PASSWORD = '123456@a'

OUTPUT_FILE = 'SAP_UDO_字段结构.xlsx'

# 调试模式 (True: 仅导出少量UDO用于测试; False: 导出全部)
DEBUG_MODE = False

# SQL 查询逻辑保持不变
SQL_QUERY = """
SELECT 
      t2.Code  UDO代码,
      t2.Name  UDO名,
      t2.Type UDO类型,
      t2.IsHeader, -- 1=主表, 0=子表
      T0.TableID 表, 
      t3.Descr 表名称,

      T0.AliasID AS 字段名, 
      T0.Descr AS [描述],

      CASE 
        WHEN T0.TypeID = 'A' AND isnull(T0.EditType,'') = '' THEN N'字母数字-定期'
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
      t0.TypeID,t0.EditType,
      CASE WHEN T0.TypeID = 'B' THEN NULL ELSE T0.EditSize END AS 长度,

      (SELECT STRING_AGG(CAST(T1.FldValue AS NVARCHAR(MAX)) + N':' + CAST(T1.Descr AS NVARCHAR(MAX)) + N';', CHAR(13) + CHAR(10)) 
       FROM UFD1 T1 
       WHERE T1.TableID = T0.TableID AND T1.FieldID = T0.FieldID) AS 可选值,        
      T0.Dflt 默认值, 
      T0.RTable 链接表,
      ISNULL(T0.NotNull, 'N') AS 必填字段

FROM CUFD T0
LEFT JOIN (
    -- 1. 获取 UDO 主表关联 (IsHeader = 1)
    SELECT Code, Name, concat('@', TableName )TableName, 
           (CASE WHEN TYPE = 1 THEN N'主数据' WHEN TYPE = 3 THEN N'单据' ELSE N'其他' END) + 
           N' (' + 
           (CASE WHEN CanDelete = 'Y' THEN N'可移除' ELSE N'不可移除' END) + N', ' +
           (CASE WHEN CanClose = 'Y' THEN N'可关闭' ELSE N'不可关闭' END) + N', ' +
           (CASE WHEN CanCancel = 'Y' THEN N'可取消' ELSE N'不可取消' END) + 
           N')' as Type,
           1 as IsHeader
    FROM OUDO
    UNION 
    -- 2. 获取 UDO 子表关联 (IsHeader = 0)
    SELECT t.Code, t.Name, concat('@', t1.TableName )TableName, 
           (CASE WHEN t.TYPE = 1 THEN N'主数据' WHEN t.TYPE = 3 THEN N'单据' ELSE N'其他' END) + 
           N' (' + 
           (CASE WHEN t.CanDelete = 'Y' THEN N'可移除' ELSE N'不可移除' END) + N', ' +
           (CASE WHEN t.CanClose = 'Y' THEN N'可关闭' ELSE N'不可关闭' END) + N', ' +
           (CASE WHEN t.CanCancel = 'Y' THEN N'可取消' ELSE N'不可取消' END) + 
           N')' as Type,
           0 as IsHeader
    FROM OUDO t
    INNER JOIN UDO1 t1 ON t.Code = t1.Code
) t2 ON T0.TableID = t2.TableName

LEFT JOIN OUTB t3 on CONCAT('@', t3.TableName)= t0.TableID
WHERE 
      1 = 1 
      and t0.TableID in (
  '@CH_ORDR','@CH_ORDR_1','@CH_ORDR_3','@CH_OQUT','@CH_OQUT_1' ,'@CH_OQUT_3'      )

ORDER BY t2.Code desc , T0.TableID, T0.FieldID;
"""


def clean_text(text):
    """
    深度清洗字符串，防止 Excel 损坏。
    """
    if not isinstance(text, str): return text
    # 移除 Excel 不支持的控制字符
    text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', text)
    # 截断超长文本
    if len(text) > 32700:
        text = text[:32700] + "..."
    # 防止公式注入
    if text.startswith('='):
        text = "'" + text
    return text


def clean_sheet_name(name, used_names_lower):
    """
    清洗并确保 Sheet 名称唯一且合法
    """
    if pd.isna(name) or name == "": name = "Unknown"
    name = str(name)
    invalid_chars = ['\\', '/', '*', '[', ']', ':', '?', '：', '？', '／', '＼', '［', '］', '【', '】']
    for char in invalid_chars:
        name = name.replace(char, ' ')
    name = name.strip()
    if not name: name = "Unknown"

    base_name = name[:25]
    candidate = base_name
    counter = 1
    while candidate.lower() in used_names_lower:
        candidate = f"{base_name}_{counter}"
        counter += 1
    used_names_lower.add(candidate.lower())
    return candidate


def get_best_driver():
    """获取最佳 SQL Server ODBC 驱动"""
    try:
        installed_drivers = pyodbc.drivers()
    except Exception:
        return None
    preferences = ['ODBC Driver 18 for SQL Server', 'ODBC Driver 17 for SQL Server',
                   'SQL Server Native Client 11.0', 'ODBC Driver 13 for SQL Server', 'SQL Server']
    for pref in preferences:
        if pref in installed_drivers: return pref
    for driver in installed_drivers:
        if 'SQL Server' in driver: return driver
    return None


def test_tcp_connection(host, port=1433):
    """网络连通性测试"""
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


def enrich_linked_table_values(df, engine):
    """
    核心增强逻辑：
    遍历数据中出现的 '设置链接表'，查询目标表的前50条数据，
    拼按 'Code-Name;' 格式填充到 '可选值' 列。
    """
    print("\n[处理链接表] 开始获取链接表数据...")

    # 筛选出有链接表的行，提取唯一的表名
    # 注意：此时 df 列名已经 rename 过了，所以用 '设置链接表'
    if '设置链接表' not in df.columns:
        print("警告：未找到 '设置链接表' 列，跳过处理。")
        return df

    # 获取所有非空且非空字符串的链接表名
    linked_tables = df[df['设置链接表'].notna() & (df['设置链接表'] != '')]['设置链接表'].unique()

    # 缓存查询结果，避免同个表重复查询
    table_cache = {}

    with engine.connect() as conn:
        for table_name in linked_tables:
            try:
                # 构造查询 SQL
                # 1. 强制使用 TOP 50 防止数据量过大撑爆 Excel 单元格
                # 2. 假设目标表都有 Code 和 Name 字段 (适用于大多数 UDO 和自定义表)
                # 3. 使用中括号 [] 包裹表名，防止表名含特殊字符报错
                sql = text(f"SELECT TOP 50 Code, Name FROM [@{table_name}] ORDER BY Code")

                result = conn.execute(sql).fetchall()

                if result:
                    # 拼接字符串: Code-Name; 换行
                    # 使用 char(13)+char(10) 在 python 中对应 \n，Excel 会识别为换行
                    value_list = [f"{str(row[0])}-{str(row[1])};" for row in result]
                    joined_str = "\n".join(value_list)

                    # 如果达到了 50 条，加个提示
                    if len(result) >= 50:
                        joined_str += "\n...(仅显示前50条)"

                    table_cache[table_name] = joined_str
                else:
                    table_cache[table_name] = "(链接表中无数据)"

            except Exception as e:
                error_msg = str(e).split(']')[0]  # 简化错误信息
                print(f"  -> 读取链接表 [{table_name}] 失败: {error_msg}")
                # 可能是没有 Code/Name 字段，或者表不存在
                table_cache[table_name] = f"(无法读取链接表: 缺少Code/Name字段或权限不足)"

    # 将缓存的数据回填到 DataFrame
    # 逻辑：如果 '设置链接表' 有值，则用查到的数据覆盖 '可选值'
    for table_name, valid_values_str in table_cache.items():
        # 找到对应链接表的所有行
        mask = df['设置链接表'] == table_name
        # 赋值
        df.loc[mask, '可选值'] = valid_values_str

    print(f"[处理链接表] 完成，共处理 {len(table_cache)} 个链接表。\n")
    return df


def export_to_excel():
    if not test_tcp_connection(DB_SERVER, 1433):
        print("警告: 网络连接测试失败，建议检查防火墙。")

    print("正在连接数据库...")
    driver_name = get_best_driver()
    if not driver_name:
        print("错误: 未找到任何 SQL Server ODBC 驱动！")
        return
    print(f"正在使用驱动程序: {driver_name}")

    server_addr = DB_SERVER
    if not server_addr.startswith('tcp:'):
        server_addr = f'tcp:{server_addr}'

    # 构造连接字符串
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
        print("正在执行主 SQL 查询...")
        df = pd.read_sql(SQL_QUERY, engine)
        print(f"查询完成，共获取 {len(df)} 行字段定义。")

        # 重命名列以匹配后续逻辑
        df = df.rename(columns={'链接表': '设置链接表', '默认值': '字段默认值'})
        if '备注' not in df.columns: df['备注'] = ""

        # ---------------------------------------------------------
        # 新增逻辑：处理链接表数据回填
        # ---------------------------------------------------------
        df = enrich_linked_table_values(df, engine)
        # ---------------------------------------------------------

        # 将“长度”列转换为 Int64 类型，去除 .0 小数位
        df['长度'] = pd.to_numeric(df['长度'], errors='coerce').astype('Int64')

        display_columns = ['字段名', '描述', '类型', '长度', '设置链接表', '可选值', '字段默认值', '必填字段', '备注']

        # --- 系统字段 (备注已置空) ---
        standard_fields_df = pd.DataFrame([
            {'字段名': 'Code', '描述': '代码', '类型': '字母数字-定期', '长度': 50, '设置链接表': '', '可选值': '',
             '字段默认值': '', '必填字段': 'Y', '备注': ''},
            {'字段名': 'Name', '描述': '名称', '类型': '字母数字-文本', '长度': 100, '设置链接表': '', '可选值': '',
             '字段默认值': '', '必填字段': 'Y', '备注': ''}
        ])

        print(f"正在生成 Excel 文件: {OUTPUT_FILE} ...")

        with pd.ExcelWriter(OUTPUT_FILE, engine='xlsxwriter',
                            engine_kwargs={'options': {'strings_to_urls': False, 'nan_inf_to_errors': True}}) as writer:
            workbook = writer.book

            # --- 样式定义 ---
            base_style = {'font_size': 10, 'valign': 'vcenter', 'font_name': 'Microsoft YaHei'}

            header_fmt = workbook.add_format(
                {**base_style, 'bold': True, 'text_wrap': True, 'align': 'center', 'fg_color': '#BFBFBF', 'border': 2})
            label_fmt = workbook.add_format(
                {**base_style, 'bold': True, 'bg_color': '#F2F2F2', 'border': 2, 'align': 'center'})
            value_fmt = workbook.add_format({**base_style, 'border': 2, 'align': 'left'})
            section_title_fmt = workbook.add_format(
                {**base_style, 'bold': True, 'bg_color': '#BFBFBF', 'border': 2, 'align': 'center'})

            # 样式：顶端对齐 (用于可选值)
            cell_fmt = workbook.add_format({**base_style, 'border': 1, 'valign': 'top', 'text_wrap': True})
            cell_left_thick_fmt = workbook.add_format(
                {**base_style, 'left': 2, 'right': 1, 'top': 1, 'bottom': 1, 'valign': 'top', 'text_wrap': True})
            cell_right_thick_fmt = workbook.add_format(
                {**base_style, 'left': 1, 'right': 2, 'top': 1, 'bottom': 1, 'valign': 'top', 'text_wrap': True})
            cell_bottom_fmt = workbook.add_format(
                {**base_style, 'left': 1, 'right': 1, 'top': 1, 'bottom': 2, 'valign': 'top', 'text_wrap': True})
            cell_bottom_left_thick_fmt = workbook.add_format(
                {**base_style, 'left': 2, 'right': 1, 'top': 1, 'bottom': 2, 'valign': 'top', 'text_wrap': True})
            cell_bottom_right_thick_fmt = workbook.add_format(
                {**base_style, 'left': 1, 'right': 2, 'top': 1, 'bottom': 2, 'valign': 'top', 'text_wrap': True})

            # 样式：垂直居中 (用于其他列)
            cell_fmt_vc = workbook.add_format({**base_style, 'border': 1, 'valign': 'vcenter', 'text_wrap': True})
            cell_left_thick_fmt_vc = workbook.add_format(
                {**base_style, 'left': 2, 'right': 1, 'top': 1, 'bottom': 1, 'valign': 'vcenter', 'text_wrap': True})
            cell_right_thick_fmt_vc = workbook.add_format(
                {**base_style, 'left': 1, 'right': 2, 'top': 1, 'bottom': 1, 'valign': 'vcenter', 'text_wrap': True})
            cell_bottom_fmt_vc = workbook.add_format(
                {**base_style, 'left': 1, 'right': 1, 'top': 1, 'bottom': 2, 'valign': 'vcenter', 'text_wrap': True})
            cell_bottom_left_thick_fmt_vc = workbook.add_format(
                {**base_style, 'left': 2, 'right': 1, 'top': 1, 'bottom': 2, 'valign': 'vcenter', 'text_wrap': True})
            cell_bottom_right_thick_fmt_vc = workbook.add_format(
                {**base_style, 'left': 1, 'right': 2, 'top': 1, 'bottom': 2, 'valign': 'vcenter', 'text_wrap': True})

            used_sheet_names_lower = set()
            valid_types = ['字母数字-定期', '字母数字-文本', '数字', '日期', '时间', '单位与总计-金额',
                           '单位与总计-价格', '单位与总计-数量', '单位与总计-百分比', '单位与总计-度量', '图片',
                           '复选框', '备注']
            type_list_formula = f"='_配置_可选值'!$A$1:$A${len(valid_types)}"
            yn_list_formula = "='_配置_可选值'!$B$1:$B$2"
            has_created_visible_sheet = False

            # --- 1. 处理 UDO (按 UDO代码 分组) ---
            df_udo = df[df['UDO代码'].notna()]
            udo_groups = list(df_udo.groupby('UDO代码'))

            if DEBUG_MODE:
                print("【调试模式开启】仅导出前 2 个 UDO...")
                udo_groups = udo_groups[:2]

            for udo_code, udo_group in udo_groups:
                udo_code = clean_text(str(udo_code))
                raw_sheet_name = udo_group.iloc[0]['UDO名']
                if pd.isna(raw_sheet_name): raw_sheet_name = udo_code
                sheet_name = clean_sheet_name(raw_sheet_name, used_sheet_names_lower)

                print(f"处理 Sheet: {sheet_name} (UDO: {udo_code})")
                # 创建 Sheet
                pd.DataFrame().to_excel(writer, sheet_name=sheet_name, startrow=0, index=False)
                worksheet = writer.sheets[sheet_name]

                if not has_created_visible_sheet:
                    worksheet.activate()
                    has_created_visible_sheet = True

                # 设置列宽
                worksheet.set_column('A:A', 2)
                worksheet.set_column('B:B', 20)
                worksheet.set_column('C:C', 35)
                worksheet.set_column('D:D', 18)
                worksheet.set_column('E:E', 8)
                worksheet.set_column('F:F', 21)
                worksheet.set_column('G:G', 32)  # 可选值列
                worksheet.set_column('H:H', 15)
                worksheet.set_column('I:I', 10)
                worksheet.set_column('J:J', 20)

                current_row = 1
                table_list = udo_group['表'].unique()
                table_type_val = udo_group.iloc[0]['UDO类型']

                for table_id in table_list:
                    table_id = clean_text(str(table_id))
                    table_data = udo_group[udo_group['表'] == table_id]
                    table_desc = table_data.iloc[0]['表名称']
                    if pd.isna(table_desc): table_desc = ""
                    table_desc = clean_text(str(table_desc))

                    is_header = table_data.iloc[0]['IsHeader']

                    # --- 绘制表头块 ---
                    worksheet.write(current_row, 1, "表名", label_fmt)
                    worksheet.write(current_row, 2, table_id, value_fmt)
                    worksheet.write(current_row, 3, "", value_fmt)
                    worksheet.write(current_row, 4, "UDO代码", label_fmt)
                    worksheet.merge_range(current_row, 5, current_row, 9, udo_code, value_fmt)

                    worksheet.write(current_row + 1, 1, "描述", label_fmt)
                    worksheet.write(current_row + 1, 2, table_desc, value_fmt)
                    worksheet.write(current_row + 1, 3, "", value_fmt)
                    worksheet.write(current_row + 1, 4, "类型", label_fmt)
                    val_type = clean_text(str(table_type_val)) if pd.notna(table_type_val) else ""
                    worksheet.merge_range(current_row + 1, 5, current_row + 1, 9, val_type, value_fmt)

                    worksheet.merge_range(current_row + 2, 1, current_row + 2, len(display_columns), "字段",
                                          section_title_fmt)

                    for col_idx, col_name in enumerate(display_columns):
                        worksheet.write(current_row + 3, col_idx + 1, col_name, header_fmt)

                    data_start_row = current_row + 4
                    data_to_write = table_data[display_columns]

                    if '主数据' in str(table_type_val) and is_header == 1:
                        data_to_write = pd.concat([standard_fields_df, data_to_write], ignore_index=True)

                    data_to_write = data_to_write.reset_index(drop=True)
                    total_rows = len(data_to_write)

                    for r_idx, row in data_to_write.iterrows():
                        actual_row = data_start_row + r_idx
                        is_last_row = (r_idx == total_rows - 1)
                        for c_idx, val in enumerate(row):
                            val = "" if pd.isna(val) else clean_text(str(val))
                            col_pos = c_idx

                            # 垂直居中判断
                            # 5: 可选值 (链接表数据可能很长，保持 Top Align)
                            use_vc = col_pos in [0, 1, 2, 3, 4, 6, 7]

                            is_left_edge = (col_pos == 0)
                            is_right_edge = (col_pos == len(display_columns) - 1)

                            if is_last_row:
                                if is_left_edge:
                                    fmt_to_use = cell_bottom_left_thick_fmt_vc if use_vc else cell_bottom_left_thick_fmt
                                elif is_right_edge:
                                    fmt_to_use = cell_bottom_right_thick_fmt_vc if use_vc else cell_bottom_right_thick_fmt
                                else:
                                    fmt_to_use = cell_bottom_fmt_vc if use_vc else cell_bottom_fmt
                            else:
                                if is_left_edge:
                                    fmt_to_use = cell_left_thick_fmt_vc if use_vc else cell_left_thick_fmt
                                elif is_right_edge:
                                    fmt_to_use = cell_right_thick_fmt_vc if use_vc else cell_right_thick_fmt
                                else:
                                    fmt_to_use = cell_fmt_vc if use_vc else cell_fmt

                            if c_idx == 3 and val.isdigit():
                                worksheet.write_number(actual_row, c_idx + 1, int(val), fmt_to_use)
                            else:
                                worksheet.write(actual_row, c_idx + 1, val, fmt_to_use)

                        worksheet.data_validation(actual_row, 3, actual_row, 3,
                                                  {'validate': 'list', 'source': type_list_formula})
                        worksheet.data_validation(actual_row, 8, actual_row, 8,
                                                  {'validate': 'list', 'source': yn_list_formula})

                    current_row += 4 + len(data_to_write) + 2

            # --- 2. 处理 非UDO (独立表) ---
            df_no_udo = df[df['UDO代码'].isna()]
            no_udo_groups = list(df_no_udo.groupby('表'))

            if DEBUG_MODE:
                no_udo_groups = no_udo_groups[:2]

            for table_id, table_group in no_udo_groups:
                table_id = clean_text(str(table_id))
                raw_sheet_name = table_group.iloc[0]['表名称']
                if pd.isna(raw_sheet_name): raw_sheet_name = table_id.replace('@', '')
                sheet_name = clean_sheet_name(raw_sheet_name, used_sheet_names_lower)
                print(f"处理 Sheet: {sheet_name} (Table: {table_id})")

                pd.DataFrame().to_excel(writer, sheet_name=sheet_name, startrow=0, index=False)
                worksheet = writer.sheets[sheet_name]

                if not has_created_visible_sheet:
                    worksheet.activate()
                    has_created_visible_sheet = True

                worksheet.set_column('A:A', 2)
                worksheet.set_column('B:B', 20)
                worksheet.set_column('C:C', 35)
                worksheet.set_column('D:D', 18)
                worksheet.set_column('E:E', 8)
                worksheet.set_column('F:F', 21)
                worksheet.set_column('G:G', 32)
                worksheet.set_column('H:H', 15)
                worksheet.set_column('I:I', 10)
                worksheet.set_column('J:J', 20)

                table_desc = clean_text(str(table_group.iloc[0]['表名称']))
                current_row = 1

                worksheet.write(current_row, 1, "表名", label_fmt)
                worksheet.write(current_row, 2, table_id, value_fmt)
                worksheet.write(current_row, 3, "", value_fmt)
                worksheet.write(current_row, 4, "UDO代码", label_fmt)
                worksheet.merge_range(current_row, 5, current_row, 9, "", value_fmt)

                worksheet.write(current_row + 1, 1, "描述", label_fmt)
                worksheet.write(current_row + 1, 2, table_desc, value_fmt)
                worksheet.write(current_row + 1, 3, "", value_fmt)
                worksheet.write(current_row + 1, 4, "类型", label_fmt)
                worksheet.merge_range(current_row + 1, 5, current_row + 1, 9, "无对象表", value_fmt)

                worksheet.merge_range(current_row + 2, 1, current_row + 2, len(display_columns), "字段",
                                      section_title_fmt)

                for col_idx, col_name in enumerate(display_columns):
                    worksheet.write(current_row + 3, col_idx + 1, col_name, header_fmt)

                data_start_row = current_row + 4
                data_to_write = table_group[display_columns]
                data_to_write = pd.concat([standard_fields_df, data_to_write], ignore_index=True)
                data_to_write = data_to_write.reset_index(drop=True)
                total_rows = len(data_to_write)

                for r_idx, row in data_to_write.iterrows():
                    actual_row = data_start_row + r_idx
                    is_last_row = (r_idx == total_rows - 1)
                    for c_idx, val in enumerate(row):
                        val = "" if pd.isna(val) else clean_text(str(val))
                        col_pos = c_idx

                        use_vc = col_pos in [0, 1, 2, 3, 4, 6, 7]

                        is_left_edge = (col_pos == 0)
                        is_right_edge = (col_pos == len(display_columns) - 1)
                        if is_last_row:
                            if is_left_edge:
                                fmt_to_use = cell_bottom_left_thick_fmt_vc if use_vc else cell_bottom_left_thick_fmt
                            elif is_right_edge:
                                fmt_to_use = cell_bottom_right_thick_fmt_vc if use_vc else cell_bottom_right_thick_fmt
                            else:
                                fmt_to_use = cell_bottom_fmt_vc if use_vc else cell_bottom_fmt
                        else:
                            if is_left_edge:
                                fmt_to_use = cell_left_thick_fmt_vc if use_vc else cell_left_thick_fmt
                            elif is_right_edge:
                                fmt_to_use = cell_right_thick_fmt_vc if use_vc else cell_right_thick_fmt
                            else:
                                fmt_to_use = cell_fmt_vc if use_vc else cell_fmt

                        if c_idx == 3 and val.isdigit():
                            worksheet.write_number(actual_row, c_idx + 1, int(val), fmt_to_use)
                        else:
                            worksheet.write(actual_row, c_idx + 1, val, fmt_to_use)

                    worksheet.data_validation(actual_row, 3, actual_row, 3,
                                              {'validate': 'list', 'source': type_list_formula})
                    worksheet.data_validation(actual_row, 8, actual_row, 8,
                                              {'validate': 'list', 'source': yn_list_formula})

            # 配置 Sheet
            ws_config = workbook.add_worksheet('_配置_可选值')
            ws_config.hide()
            ws_config.write_column('A1', valid_types)
            ws_config.write_column('B1', ['Y', 'N'])

        print(f"\n成功! 文件已保存为: {OUTPUT_FILE}")

    except Exception as e:
        print(f"\n发生错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    export_to_excel()