"""
Generate a comprehensive tutorial PDF from the Dataanalysis project.
Covers all knowledge points for a beginner-friendly learning guide.
"""

from fpdf import FPDF
import os


class TutorialPDF(FPDF):
    def __init__(self):
        super().__init__()
        font_path = r"C:\Windows\Fonts"
        self.add_font("SimHei", "", os.path.join(font_path, "simhei.ttf"), uni=True)
        self.add_font("SimHei", "B", os.path.join(font_path, "simhei.ttf"), uni=True)
        self.add_font("SimKai", "", os.path.join(font_path, "simkai.ttf"), uni=True)
        self.set_auto_page_break(auto=True, margin=20)

    def header(self):
        if self.page_no() > 1:
            self.set_font("SimHei", "", 8)
            self.set_text_color(128, 128, 128)
            self.cell(0, 8, "电商数据分析实战教程 - 知识点全解", align="C")
            self.ln(4)
            self.set_draw_color(200, 200, 200)
            self.line(10, self.get_y(), 200, self.get_y())
            self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_font("SimHei", "", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f"第 {self.page_no()} 页", align="C")

    def chapter_title(self, num, title):
        self.set_font("SimHei", "B", 18)
        self.set_text_color(30, 60, 120)
        self.ln(6)
        self.cell(0, 12, f"第 {num} 章  {title}", new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(30, 60, 120)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(6)

    def section_title(self, title):
        self.set_font("SimHei", "B", 14)
        self.set_text_color(50, 90, 160)
        self.ln(4)
        self.cell(0, 10, title, new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def subsection_title(self, title):
        self.set_font("SimHei", "B", 12)
        self.set_text_color(70, 70, 70)
        self.ln(2)
        self.cell(0, 8, title, new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def body_text(self, text):
        self.set_font("SimHei", "", 10.5)
        self.set_text_color(40, 40, 40)
        self.multi_cell(0, 6.5, text)
        self.ln(2)

    def tip_box(self, title, text):
        self.set_fill_color(230, 245, 255)
        self.set_draw_color(70, 130, 200)
        x = self.get_x()
        y = self.get_y()
        self.set_font("SimHei", "B", 10)
        self.set_text_color(30, 80, 160)
        # Calculate height needed
        self.set_xy(x + 4, y + 4)
        self.cell(0, 6, f"💡 {title}")
        self.ln(8)
        self.set_x(x + 4)
        self.set_font("SimHei", "", 10)
        self.set_text_color(40, 40, 40)
        self.multi_cell(180, 6, text)
        end_y = self.get_y() + 4
        # Draw box
        self.rect(x, y, 190, end_y - y, style="D")
        self.set_y(end_y + 4)

    def warning_box(self, text):
        self.set_fill_color(255, 245, 230)
        self.set_draw_color(200, 150, 50)
        x = self.get_x()
        y = self.get_y()
        self.set_font("SimHei", "B", 10)
        self.set_text_color(180, 100, 0)
        self.set_xy(x + 4, y + 4)
        self.cell(0, 6, "⚠️ 注意")
        self.ln(8)
        self.set_x(x + 4)
        self.set_font("SimHei", "", 10)
        self.set_text_color(60, 40, 0)
        self.multi_cell(180, 6, text)
        end_y = self.get_y() + 4
        self.rect(x, y, 190, end_y - y, style="D")
        self.set_y(end_y + 4)

    def code_block(self, code):
        self.set_fill_color(245, 245, 245)
        self.set_draw_color(180, 180, 180)
        x = self.get_x()
        y = self.get_y()
        self.set_font("SimKai", "", 9)
        self.set_text_color(30, 30, 30)
        self.set_xy(x + 4, y + 4)
        lines = code.strip().split("\n")
        for line in lines:
            self.set_x(x + 4)
            self.cell(182, 5, line, new_x="LMARGIN", new_y="NEXT")
        end_y = self.get_y() + 4
        self.rect(x, y, 190, end_y - y, style="DF")
        self.set_y(end_y + 4)

    def table_header(self, cols, widths):
        self.set_fill_color(30, 60, 120)
        self.set_text_color(255, 255, 255)
        self.set_font("SimHei", "B", 9.5)
        for i, col in enumerate(cols):
            self.cell(widths[i], 8, col, border=1, fill=True, align="C")
        self.ln()

    def table_row(self, cells, widths, highlight=False):
        if highlight:
            self.set_fill_color(240, 248, 255)
        else:
            self.set_fill_color(255, 255, 255)
        self.set_text_color(40, 40, 40)
        self.set_font("SimHei", "", 9)
        for i, cell in enumerate(cells):
            self.cell(widths[i], 7, str(cell), border=1, fill=True, align="C")
        self.ln()

    def bullet(self, text, indent=10):
        self.set_font("SimHei", "", 10.5)
        self.set_text_color(40, 40, 40)
        x = self.get_x()
        self.set_x(x + indent)
        self.cell(5, 6.5, "•")
        self.multi_cell(0, 6.5, text)
        self.ln(1)


def build_pdf():
    pdf = TutorialPDF()
    pdf.set_title("电商数据分析实战教程")
    pdf.set_author("Dataanalysis Project")

    # ========== Cover Page ==========
    pdf.add_page()
    pdf.ln(50)
    pdf.set_font("SimHei", "B", 28)
    pdf.set_text_color(30, 60, 120)
    pdf.cell(0, 15, "电商数据分析实战教程", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(8)
    pdf.set_font("SimHei", "", 16)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 10, "——从零到一掌握 RFM-I 用户分层与 A/B 实验", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(30)
    pdf.set_font("SimHei", "", 12)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 8, "基于 Dataanalysis 项目全部代码知识点逐行拆解", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, "适合零基础 / 基础薄弱的数据分析学习者", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(40)
    pdf.set_draw_color(30, 60, 120)
    pdf.line(60, pdf.get_y(), 150, pdf.get_y())
    pdf.ln(10)
    pdf.set_font("SimHei", "", 11)
    pdf.cell(0, 8, "涵盖技术栈：Python · Pandas · NumPy · DuckDB · SQL · scipy · A/B Test", align="C")

    # ========== Table of Contents ==========
    pdf.add_page()
    pdf.set_font("SimHei", "B", 20)
    pdf.set_text_color(30, 60, 120)
    pdf.cell(0, 12, "目  录", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(10)

    toc = [
        ("第一章", "项目概述与技术栈", "了解项目背景、数据来源、整体架构"),
        ("第二章", "Python 基础知识", "import、变量、数据类型、函数、lambda"),
        ("第三章", "NumPy 核心用法", "随机数、数组操作、统计分布"),
        ("第四章", "Pandas 数据处理（上）", "DataFrame、读取数据、数据筛选、空值处理"),
        ("第五章", "Pandas 数据处理（下）", "分箱(qcut/cut)、groupby、apply、格式化"),
        ("第六章", "SQL 与 DuckDB", "CASE WHEN、CTE(WITH)、JOIN、子查询、聚合"),
        ("第七章", "RFM 模型与特征工程", "RFM 原理、I 指标引入、用户分层逻辑"),
        ("第八章", "数据可视化基础", "matplotlib、seaborn、Power BI 概述"),
        ("第九章", "A/B 实验与统计检验", "实验设计、卡方检验、T 检验、p 值解读"),
        ("第十章", "业务思维与落地应用", "帕累托法则、MECE 原则、ROI、运营策略"),
    ]

    for num, title, desc in toc:
        pdf.set_font("SimHei", "B", 12)
        pdf.set_text_color(30, 60, 120)
        pdf.cell(25, 8, num)
        pdf.set_font("SimHei", "B", 12)
        pdf.set_text_color(40, 40, 40)
        pdf.cell(80, 8, title)
        pdf.set_font("SimHei", "", 10)
        pdf.set_text_color(120, 120, 120)
        pdf.cell(0, 8, desc, new_x="LMARGIN", new_y="NEXT")
        pdf.ln(3)

    # ========== Chapter 1: Project Overview ==========
    pdf.add_page()
    pdf.chapter_title("一", "项目概述与技术栈")

    pdf.section_title("1.1 项目背景")
    pdf.body_text(
        "在电商运营中，传统的 RFM 模型仅通过"结果性数据"（交易金额与频次）对用户分类，"
        "难以捕捉用户在转化前的"过程性行为"。本项目通过分析千万级用户行为日志，引入"
        "意向度（Intent）指标，重构为 RFM-I 模型，精准识别高潜但未转化的用户。"
    )

    pdf.section_title("1.2 数据来源")
    pdf.body_text(
        "数据集名称：eCommerce behavior data from multi category store\n"
        "数据来源：Kaggle (REES46)\n"
        "数据体量：2019 年 10-11 月完整行为日志，约 13.7GB，记录数千万条\n"
        "核心字段：event_time, event_type, product_id, price, user_id, user_session 等"
    )

    pdf.section_title("1.3 技术栈选型")
    cols = ["组件", "技术选型", "说明"]
    w = [40, 50, 100]
    pdf.table_header(cols, w)
    rows = [
        ["分析引擎", "DuckDB", "嵌入式分析型数据库，适合单机大数据分析"],
        ["编程语言", "Python 3.12", "主力数据处理语言"],
        ["核心库", "pandas / numpy", "数据清洗、特征工程、统计计算"],
        ["统计检验", "scipy.stats", "卡方检验、T 检验等假设检验"],
        ["可视化", "matplotlib/seaborn", "静态图表绘制"],
        ["BI 工具", "Power BI", "交互式仪表板与业务报表"],
        ["DB 管理", "DBeaver", "数据库 GUI 管理工具"],
    ]
    for i, row in enumerate(rows):
        pdf.table_row(row, w, highlight=(i % 2 == 0))

    pdf.section_title("1.4 项目架构流程")
    pdf.body_text(
        "1) 原始 CSV 数据（13GB）→ DuckDB 抽样 1%\n"
        "2) SQL 特征工程 → 构建用户特征宽表（RFM + Intent + Session）\n"
        "3) Python 数据清洗 → 时间转换、空值处理\n"
        "4) 特征分箱打分 → RFM-I 评分体系\n"
        "5) 用户分层标签 → 成交客 6 类 + 潜客 5 类\n"
        "6) Power BI 可视化 → 业务仪表板\n"
        "7) A/B 实验验证 → 优惠券策略效果评估"
    )

    # ========== Chapter 2: Python Basics ==========
    pdf.add_page()
    pdf.chapter_title("二", "Python 基础知识")

    pdf.section_title("2.1 import 导入模块")
    pdf.body_text(
        "Python 的强大来自于丰富的第三方库。使用 import 语句导入库后，"
        "就可以调用库中提供的各种功能。"
    )
    pdf.code_block(
        "import numpy as np          # 导入 NumPy，起别名 np\n"
        "import pandas as pd         # 导入 Pandas，起别名 pd\n"
        "from scipy import stats     # 从 scipy 库中只导入 stats 模块\n"
        "import duckdb               # 导入 DuckDB\n"
        "import matplotlib.pyplot as plt   # 导入绘图库\n"
        "import seaborn as sns       # 导入统计可视化库"
    )
    pdf.tip_box("为什么要用别名？",
        "as np / as pd 是约定俗成的别名，写代码时用 np.xxx 代替 numpy.xxx，更简洁。\n"
        "from ... import ... 是只导入库中某个子模块，避免导入整个库节省内存。")

    pdf.section_title("2.2 变量与数据类型")
    pdf.body_text(
        "变量就是给一个值起个名字，方便后续使用。Python 中不需要声明类型，"
        "会自动推断。"
    )
    pdf.code_block(
        "TOTAL_USERS = 1390000       # 整数 (int)\n"
        "cvr_A = 0.11                # 浮点数 (float)\n"
        "group_labels = ['A', 'B']   # 列表 (list)\n"
        "name = '核心高价值客户'      # 字符串 (str)\n"
        "is_active = True            # 布尔值 (bool)"
    )

    pdf.section_title("2.3 函数定义 (def)")
    pdf.body_text(
        "函数是一段可以重复调用的代码块。用 def 关键字定义，"
        "可以接收参数，也可以返回值。"
    )
    pdf.code_block(
        "def get_active_segment(row):\n"
        "    r = row['R_score']\n"
        "    f = row['F_score']\n"
        "    m = row['M_score']\n"
        "    i = row['I_score']\n"
        "    if m >= 5 and r >= 4:\n"
        "        return '核心高价值客户'\n"
        "    elif m >= 5 and r <= 2:\n"
        "        return '重要挽留客户'\n"
        "    else:\n"
        "        return '一般客户'"
    )
    pdf.tip_box("逐行解读",
        "• def 定义函数，row 是参数（代表传入的每一行数据）\n"
        "• row['R_score'] 是用键名从字典/Series 中取值\n"
        "• if/elif/else 是条件判断，按顺序检查，命中即返回\n"
        "• return 表示函数的输出结果")

    pdf.section_title("2.4 lambda 匿名函数")
    pdf.body_text(
        "lambda 是一种简短的"一次性"函数，常用于 apply 和格式化场景。"
    )
    pdf.code_block(
        "# 将小数转为百分比格式字符串\n"
        "display_df['人数占比'] = display_df['人数占比'].apply(\n"
        "    lambda x: f\"{x:.2%}\"\n"
        ")\n"
        "# 等价于:\n"
        "def format_pct(x):\n"
        "    return f\"{x:.2%}\"\n"
        "display_df['人数占比'] = display_df['人数占比'].apply(format_pct)"
    )

    pdf.section_title("2.5 f-string 格式化字符串")
    pdf.body_text(
        "f-string 是 Python 3.6+ 的字符串格式化方式，在字符串前加 f，"
        "用 {} 嵌入变量或表达式。"
    )
    pdf.code_block(
        "p_val = 0.00001234\n"
        "print(f\"p-value: {p_val:.4e}\")   # 科学计数法，保留4位 → 1.2340e-05\n"
        "print(f\"转化率: {0.135:.2%}\")     # 百分比，保留2位 → 13.50%\n"
        "print(f\"总计: {12345:,}\")          # 千位分隔符 → 12,345"
    )

    # ========== Chapter 3: NumPy ==========
    pdf.add_page()
    pdf.chapter_title("三", "NumPy 核心用法")

    pdf.section_title("3.1 什么是 NumPy？")
    pdf.body_text(
        "NumPy（Numerical Python）是 Python 科学计算的基础库，提供高效的多维数组"
        "对象和数学运算函数。在数据分析中，它主要用于：\n"
        "• 生成随机数（模拟数据、抽样）\n"
        "• 数学运算（统计、线性代数）\n"
        "• 与 Pandas 配合使用（Pandas 底层就是 NumPy）"
    )

    pdf.section_title("3.2 随机数种子 (np.random.seed)")
    pdf.body_text(
        "设置随机种子可以确保每次运行代码生成的随机数相同，保证实验可复现。"
    )
    pdf.code_block(
        "np.random.seed(42)  # 设定种子为42\n"
        "# 之后所有随机操作都会产生相同的结果"
    )
    pdf.tip_box("为什么需要固定种子？",
        "在 A/B 测试中，我们需要多次运行代码验证结果。如果随机数每次不同，"
        "结果就无法复现，其他人也无法验证你的实验。固定种子 = 固定随机序列。")

    pdf.section_title("3.3 np.random.choice — 随机选择")
    pdf.body_text(
        "从给定数组中随机抽取元素，可以指定概率权重。"
    )
    pdf.code_block(
        "# 从 ['A','B'] 中随机抽取 1390000 次，各 50% 概率\n"
        "group_labels = np.random.choice(\n"
        "    ['A', 'B'],              # 可选值\n"
        "    size=TOTAL_USERS,        # 抽取次数\n"
        "    p=[0.5, 0.5]             # 各自概率\n"
        ")"
    )

    pdf.section_title("3.4 np.random.binomial — 二项分布")
    pdf.body_text(
        "二项分布模拟"做 N 次实验，成功概率为 p，成功几次"的场景。"
        "在本项目中，模拟每个用户是否转化（1次实验，成功=转化）。"
    )
    pdf.code_block(
        "# 每个用户做1次实验，转化概率 11%\n"
        "# 返回 0（未转化）或 1（已转化）\n"
        "df.loc[mask_A, 'is_converted'] = np.random.binomial(\n"
        "    n=1,            # 实验次数（1次=伯努利试验）\n"
        "    p=cvr_A,        # 成功概率 0.11\n"
        "    size=mask_A.sum()  # 生成数量\n"
        ")"
    )
    pdf.tip_box("伯努利试验",
        "n=1 的二项分布就是"伯努利试验"——最简单的随机实验：抛硬币（正/反）、"
        "用户转化（是/否）。大量伯努利试验的平均值趋近于概率 p（大数定律）。")

    pdf.section_title("3.5 np.random.normal — 正态分布")
    pdf.body_text(
        "正态分布（钟形曲线）是最常见的连续概率分布。大部分数据集中在均值附近。"
    )
    pdf.code_block(
        "# 生成客单价：均值60元，标准差20元\n"
        "amounts = np.random.normal(\n"
        "    loc=60,     # 均值 μ\n"
        "    scale=20,   # 标准差 σ\n"
        "    size=1000   # 生成数量\n"
        ")"
    )
    pdf.body_text(
        "正态分布的 68-95-99.7 法则：\n"
        "• 68% 的数据落在 [μ-σ, μ+σ] 区间\n"
        "• 95% 的数据落在 [μ-2σ, μ+2σ] 区间\n"
        "• 99.7% 的数据落在 [μ-3σ, μ+3σ] 区间"
    )

    pdf.section_title("3.6 np.clip — 截断数组")
    pdf.body_text(
        "将数组中的值限制在指定范围内，超出范围的值会被"夹"到边界。"
        "用于防止出现不合理的负数订单金额。"
    )
    pdf.code_block(
        "# 将金额限制在最低 5 元（不会出现负数或零）\n"
        "df.loc[converted_A, 'order_amount'] = np.clip(\n"
        "    np.random.normal(60, 20, size=converted_A.sum()),\n"
        "    5,      # 最小值\n"
        "    None    # 最大值不限\n"
        ")"
    )

    # ========== Chapter 4: Pandas (Part 1) ==========
    pdf.add_page()
    pdf.chapter_title("四", "Pandas 数据处理（上）")

    pdf.section_title("4.1 什么是 Pandas？")
    pdf.body_text(
        "Pandas 是 Python 最核心的数据分析库，提供 DataFrame（数据框）结构，"
        "可以理解为"编程版的 Excel 表格"。它能完成数据读取、清洗、转换、"
        "聚合、导出等几乎所有数据处理工作。"
    )

    pdf.section_title("4.2 DataFrame — 数据框")
    pdf.body_text(
        "DataFrame 是 Pandas 的核心数据结构，由行（index）和列（columns）组成，"
        "每列可以是不同的数据类型。"
    )
    pdf.code_block(
        "# 创建 DataFrame\n"
        "df = pd.DataFrame({\n"
        "    'user_id': range(1, 1000001),   # 1到100万\n"
        "    'group': group_labels,           # 随机分组标签\n"
        "    'is_converted': 0                # 初始值全为0\n"
        "})\n"
        "\n"
        "# 常用查看方法\n"
        "df.head()      # 查看前5行\n"
        "df.info()      # 查看列名、数据类型、非空数量\n"
        "df.describe()  # 查看数值列的统计摘要"
    )

    pdf.section_title("4.3 数据读取 — 从 DuckDB 读取")
    pdf.body_text(
        "DuckDB 是一个嵌入式分析型数据库（类似 SQLite 但专为分析优化），"
        "可以直接用 SQL 查询并导出为 Pandas DataFrame。"
    )
    pdf.code_block(
        "import duckdb\n"
        "con = duckdb.connect(r'D:\\project\\project.duckdb')\n"
        "df = con.execute('SELECT * FROM user_features').df()\n"
        "# .df() 将查询结果直接转为 Pandas DataFrame"
    )

    pdf.section_title("4.4 布尔索引 — 数据筛选")
    pdf.body_text(
        "布尔索引是 Pandas 最常用的数据筛选方式。用条件表达式生成 True/False "
        "的序列，再用它来筛选行。"
    )
    pdf.code_block(
        "# 筛选成交用户：Frequency > 0\n"
        "df_active = df[df['Frequency'] > 0].copy()\n"
        "\n"
        "# 筛选潜客：Frequency == 0\n"
        "df_potential = df[df['Frequency'] == 0].copy()\n"
        "\n"
        "# 多条件筛选（& 表示且，| 表示或）\n"
        "converted_A = mask_A & (df['is_converted'] == 1)"
    )
    pdf.tip_box("为什么用 .copy()？",
        "Pandas 的筛选操作默认返回"视图"（view），修改视图会影响原数据。"
        "加 .copy() 创建独立副本，避免修改原 DataFrame 时出现警告或意外。")

    pdf.section_title("4.5 loc 定位器 — 按标签赋值")
    pdf.body_text(
        "df.loc[行条件, 列名] 可以同时筛选行和指定列，进行赋值操作。"
    )
    pdf.code_block(
        "# 对 A 组用户，将 is_converted 列设为随机二项分布结果\n"
        "df.loc[mask_A, 'is_converted'] = np.random.binomial(1, cvr_A, size=mask_A.sum())\n"
        "\n"
        "# 对已转化的 A 组用户，设置订单金额\n"
        "df.loc[converted_A, 'order_amount'] = np.clip(\n"
        "    np.random.normal(60, 20, size=converted_A.sum()), 5, None\n"
        ")\n"
        "\n"
        "# 对已转化的 A 组用户，设置用券成本\n"
        "df.loc[converted_A, 'coupon_cost'] = 5"
    )

    pdf.section_title("4.6 空值处理 (fillna)")
    pdf.body_text(
        "数据中经常有缺失值（NaN/None），需要根据业务逻辑填充合理的默认值。"
    )
    pdf.code_block(
        "# Recency 空值填 99999（表示很久没来，RFM 排序时排最后）\n"
        "df['R_days'] = df['R_days'].fillna(99999)\n"
        "\n"
        "# 会话时长空值填 0（表示没有会话记录）\n"
        "df['session_seconds'] = df['session_seconds'].fillna(0)"
    )
    pdf.tip_box("fillna 的业务逻辑",
        "空值填什么取决于业务场景：\n"
        "• R_days 填大数 → 没买过的用户排在"最不活跃"的位置\n"
        "• session_seconds 填 0 → 没有会话记录就是 0 秒\n"
        "• 不能随便填 0 或平均值，要根据业务含义决定")

    # ========== Chapter 5: Pandas (Part 2) ==========
    pdf.add_page()
    pdf.chapter_title("五", "Pandas 数据处理（下）")

    pdf.section_title("5.1 时间差计算 (.dt.days / .dt.total_seconds)")
    pdf.body_text(
        "Pandas 的 .dt 访问器可以提取日期时间的各种属性。"
    )
    pdf.code_block(
        "# 计算距最近一次购买的天数差\n"
        "reference_date = df['Recency'].max()     # 取最大日期作为参考点\n"
        "diff = reference_date - df['Recency']    # 时间差（Timedelta）\n"
        "df['R_days'] = diff.dt.days              # 提取天数（整数）\n"
        "\n"
        "# 将时长转为总秒数\n"
        "df['session_seconds'] = df['avg_session'].dt.total_seconds()"
    )
    pdf.body_text(
        ".dt.seconds vs .dt.total_seconds() 的区别：\n"
        "• .dt.seconds：只返回"秒"部分（忽略天、小时、分钟）\n"
        "• .dt.total_seconds()：返回完整的总秒数\n"
        "例：1天2小时3分4秒 → .dt.seconds = 4，.dt.total_seconds() = 93784.0"
    )

    pdf.section_title("5.2 pd.qcut — 等频分箱（分位数）")
    pdf.body_text(
        "qcut 按分位数将数据分成等频的几组（每组样本量大致相同）。"
        "适合数据分布不均匀的场景。"
    )
    pdf.code_block(
        "# 将 R_days 分成 5 等份，每份约 20% 的用户\n"
        "# labels=[5,4,3,2,1] 表示天数越小（越近）分数越高\n"
        "df_active['R_score'] = pd.qcut(\n"
        "    df_active['R_days'],    # 要分箱的数据\n"
        "    q=5,                    # 分成 5 组\n"
        "    labels=[5,4,3,2,1]     # 每组的标签\n"
        ")"
    )
    pdf.tip_box("qcut vs cut 的区别",
        "• pd.qcut：等频分箱，每组样本数相近（按百分位数切）\n"
        "• pd.cut：等距分箱，每组区间宽度相同（按数值范围切）\n"
        "选择依据：qcut 适合分布不均匀的数据；cut 适合有明确业务阈值的场景")

    pdf.section_title("5.3 pd.cut — 等距分箱（自定义区间）")
    pdf.body_text(
        "cut 按照自定义的区间边界进行分箱，适合有明确业务含义的阈值。"
    )
    pdf.code_block(
        "# F_score：按购买频次分箱\n"
        "F_bins = [0, 1, 2, 3, 4, np.inf]  # np.inf 表示正无穷大\n"
        "df_active['F_score'] = pd.cut(\n"
        "    df_active['Frequency'],\n"
        "    bins=F_bins,                # 自定义区间边界\n"
        "    labels=[1, 2, 3, 4, 5]     # 每个区间的标签\n"
        ")\n"
        "\n"
        "# M_score：消费金额分 6 档（突破传统 5 分制）\n"
        "M_bins = [0, 100, 200, 400, 900, 2000, np.inf]\n"
        "df_active['M_score'] = pd.cut(\n"
        "    df_active['monetary'],\n"
        "    bins=M_bins,\n"
        "    labels=[1, 2, 3, 4, 5, 6]\n"
        ")"
    )
    pdf.body_text(
        "np.inf 表示正无穷大，确保所有数据都能落入某个区间，不会被遗漏。\n"
        "例如 bins=[0, 100, np.inf] 会创建 [0,100) 和 [100,+∞) 两个区间。"
    )

    pdf.section_title("5.4 rank 排名 — 处理重复值")
    pdf.body_text(
        "当数据中有很多相同值时（如 Intent 都是 1 或 2），直接 qcut 可能报错。"
        "先用 rank(method='first') 给每个值分配唯一排名，再分箱。"
    )
    pdf.code_block(
        "# Intent 可能有很多重复值，先排名再分箱\n"
        "df_active['I_score'] = pd.qcut(\n"
        "    df_active['Intent'].rank(method='first'),  # 先排名\n"
        "    q=5,\n"
        "    labels=[1, 2, 3, 4, 5]\n"
        ")\n"
        "# method='first'：相同值按出现顺序排名"
    )

    pdf.section_title("5.5 groupby + agg — 分组聚合")
    pdf.body_text(
        "groupby 是 Pandas 最强大的功能之一，类似 SQL 的 GROUP BY，"
        "可以按某列分组后对其他列做聚合计算。"
    )
    pdf.code_block(
        "metrics = df.groupby('group').agg(\n"
        "    users=('user_id', 'count'),          # 统计人数\n"
        "    conversions=('is_converted', 'sum'),  # 统计转化人数\n"
        "    total_revenue=('order_amount', 'sum'),# 求总金额\n"
        "    total_cost=('coupon_cost', 'sum')     # 求总成本\n"
        ").reset_index()  # 将分组列还原为普通列\n"
        "\n"
        "# 计算派生指标\n"
        "metrics['CVR'] = metrics['conversions'] / metrics['users']\n"
        "metrics['AOV'] = metrics['total_revenue'] / metrics['conversions']\n"
        "metrics['ROI'] = (metrics['total_revenue'] - metrics['total_cost']) / metrics['total_cost']"
    )
    pdf.tip_box("agg 的语法",
        "agg(新列名=(原列名, 聚合函数)) 的命名聚合语法：\n"
        "• 'count' → 计数\n"
        "• 'sum' → 求和\n"
        "• 'mean' → 求平均\n"
        "• 'nunique' → 去重计数（统计不重复的个数）\n"
        "• 'max' / 'min' → 最大/最小值")

    pdf.section_title("5.6 apply — 逐行应用函数")
    pdf.body_text(
        "apply 可以对 DataFrame 的每一行（或每一列）应用自定义函数。"
        "配合 axis=1 实现逐行计算，是自定义分层标签的核心方法。"
    )
    pdf.code_block(
        "# 将分层函数应用到每一行\n"
        "df_active['用户标签'] = df_active.apply(get_active_segment, axis=1)\n"
        "# axis=1 表示逐行应用（axis=0 是逐列）\n"
        "# get_active_segment 是之前定义的分层函数"
    )

    pdf.section_title("5.7 pd.set_option — 显示设置")
    pdf.body_text(
        "控制 Pandas 的显示行为，确保中文和长数据正确显示。"
    )
    pdf.code_block(
        "pd.set_option('display.max_columns', None)    # 显示所有列\n"
        "pd.set_option('display.width', 1000)           # 显示宽度\n"
        "pd.set_option('display.unicode.ambiguous_as_wide', True)  # 中文对齐\n"
        "pd.set_option('display.unicode.east_asian_width', True)   # 东亚字符宽度"
    )

    # ========== Chapter 6: SQL & DuckDB ==========
    pdf.add_page()
    pdf.chapter_title("六", "SQL 与 DuckDB")

    pdf.section_title("6.1 什么是 DuckDB？")
    pdf.body_text(
        "DuckDB 是一个嵌入式分析型数据库，特点：\n"
        "• 无需安装服务器，直接用 Python 连接 .duckdb 文件\n"
        "• 专为 OLAP（分析查询）优化，处理亿级数据很快\n"
        "• 支持标准 SQL 语法\n"
        "• 可以直接读取 CSV 文件进行查询"
    )
    pdf.code_block(
        "import duckdb\n"
        "con = duckdb.connect(database='project.duckdb')  # 连接数据库\n"
        "con.execute('SELECT * FROM table_name')          # 执行 SQL\n"
        "con.close()                                      # 关闭连接"
    )

    pdf.section_title("6.2 伯努利抽样 (USING SAMPLE)")
    pdf.body_text(
        "当数据量太大（13GB）时，需要抽样分析。DuckDB 支持用 SQL 直接抽样。"
    )
    pdf.code_block(
        "CREATE TABLE sample_events AS\n"
        "SELECT * FROM read_csv_auto('path/*.csv')\n"
        "USING SAMPLE 1% (bernoulli);\n"
        "-- read_csv_auto：自动识别 CSV 结构\n"
        "-- SAMPLE 1%：抽取 1% 的数据\n"
        "-- bernoulli：伯努利抽样（每行独立 1% 概率被选中）"
    )

    pdf.section_title("6.3 CASE WHEN — 条件表达式")
    pdf.body_text(
        "CASE WHEN 是 SQL 中的 if-else 逻辑，用于条件判断和数据转换。"
    )
    pdf.code_block(
        "SELECT user_id,\n"
        "    MAX(CASE WHEN event_type = 'purchase'\n"
        "             THEN event_time ELSE NULL END) AS Recency,\n"
        "    COUNT(CASE WHEN event_type = 'purchase'\n"
        "               THEN 1 ELSE NULL END) AS Frequency,\n"
        "    SUM(CASE WHEN event_type = 'purchase'\n"
        "             THEN price ELSE 0 END) AS monetary\n"
        "FROM sample_events\n"
        "GROUP BY user_id"
    )
    pdf.tip_box("CASE WHEN 的工作原理",
        "1) WHEN event_type = 'purchase' → 检查条件\n"
        "2) THEN event_time → 条件成立时返回这个值\n"
        "3) ELSE NULL → 条件不成立返回 NULL\n"
        "4) 配合 MAX：忽略 NULL，只取有值的最大值（即最近购买时间）\n"
        "5) 配合 COUNT：NULL 不计数，只统计有值的行数（即购买次数）")

    pdf.section_title("6.4 WITH (CTE) — 公共表表达式")
    pdf.body_text(
        "CTE（Common Table Expression）用 WITH 关键字定义临时命名查询，"
        "让复杂 SQL 更清晰、更易维护。"
    )
    pdf.code_block(
        "WITH t1 AS (\n"
        "    -- 第一个临时表：用户交易特征\n"
        "    SELECT user_id, ...\n"
        "    FROM sample_events\n"
        "    GROUP BY user_id\n"
        "),\n"
        "t2 AS (\n"
        "    -- 第二个临时表：会话时长\n"
        "    SELECT user_id, ...\n"
        "    FROM sample_events\n"
        "    GROUP BY user_id\n"
        ")\n"
        "-- 主查询：合并两个临时表\n"
        "SELECT t1.*, t2.avg_session\n"
        "FROM t1 LEFT JOIN t2 ON t1.user_id = t2.user_id"
    )
    pdf.body_text(
        "CTE 的优势：\n"
        "• 每个 WITH 子句独立定义，逻辑清晰\n"
        "• 方便调试（可以单独执行某个 CTE 查看结果）\n"
        "• 可以被主查询多次引用"
    )

    pdf.section_title("6.5 JOIN — 表连接")
    pdf.body_text(
        "JOIN 用于将两个表的数据按关联条件合并。"
    )
    pdf.code_block(
        "-- LEFT JOIN：保留左表所有行，右表无匹配则填 NULL\n"
        "SELECT t1.*, t2.avg_session\n"
        "FROM t1 LEFT JOIN t2 ON t1.user_id = t2.user_id\n"
        "\n"
        "-- 常见 JOIN 类型：\n"
        "-- INNER JOIN：只保留两表都有的行\n"
        "-- LEFT JOIN：保留左表所有行\n"
        "-- RIGHT JOIN：保留右表所有行\n"
        "-- FULL JOIN：保留所有行"
    )

    pdf.section_title("6.6 子查询")
    pdf.body_text(
        "子查询是嵌套在另一个查询内部的 SELECT 语句。"
    )
    pdf.code_block(
        "-- 先算每个 session 的时长，再算每个用户的平均时长\n"
        "SELECT user_id, AVG(session_duration) AS avg_session\n"
        "FROM (\n"
        "    -- 子查询：每个 session 的时长\n"
        "    SELECT user_id,\n"
        "           (MAX(event_time) - MIN(event_time)) AS session_duration\n"
        "    FROM sample_events\n"
        "    WHERE user_session IS NOT NULL\n"
        "    GROUP BY user_id, user_session\n"
        ") A  -- 子查询必须有别名\n"
        "GROUP BY user_id"
    )

    pdf.section_title("6.7 UNION ALL — 合并结果集")
    pdf.body_text(
        "UNION ALL 将两个查询的结果上下拼接（不去重）。"
    )
    pdf.code_block(
        "SELECT * FROM PotentialTags   -- 潜客标签\n"
        "UNION ALL\n"
        "SELECT * FROM ActiveTags      -- 成交客标签"
    )

    pdf.section_title("6.8 COPY — 导出数据")
    pdf.body_text(
        "DuckDB 的 COPY 命令可以将查询结果直接导出为 CSV 文件。"
    )
    pdf.code_block(
        "COPY (\n"
        "    SELECT user_id FROM full_user_tags\n"
        "    WHERE 用户标签 = '核心高意向潜客'\n"
        ") TO 'output.csv' (HEADER, DELIMITER ',');\n"
        "-- HEADER：包含列名\n"
        "-- DELIMITER ','：用逗号分隔"
    )

    # ========== Chapter 7: RFM Model ==========
    pdf.add_page()
    pdf.chapter_title("七", "RFM 模型与特征工程")

    pdf.section_title("7.1 什么是 RFM 模型？")
    pdf.body_text(
        "RFM 模型是用户价值分析的经典框架，通过三个维度衡量用户价值：\n\n"
        "• R (Recency) — 最近一次消费时间：多久没来了？越近越好\n"
        "• F (Frequency) — 消费频次：买了多少次？越多越忠诚\n"
        "• M (Monetary) — 消费金额：花了多少钱？越多越有价值\n\n"
        "三个维度组合，可以将用户分为高价值、潜力、流失等不同类型。"
    )

    pdf.section_title("7.2 为什么需要 RFM-I？")
    pdf.body_text(
        "传统 RFM 的局限：\n"
        "• 只看交易结果，忽略了大量"逛了但没买"的潜客\n"
        "• F（频次）在长尾分布中区分度低（95% 用户只买 1 次）\n\n"
        "RFM-I 的改进：引入 I (Intent) 加权意向度指标\n"
        "• I = 浏览(view)次数 × 1 + 加购(cart)次数 × 3（权重待校准）\n"
        "• cart（加购）的购买意向远强于 view（浏览），加权后更准确衡量用户行为活跃度"
    )

    pdf.section_title("7.3 特征工程流程")
    pdf.body_text(
        "步骤 1：数据转换 — 将日期/时长转为可计算的数值\n"
        "步骤 2：人群切分 — 按 Frequency 分为成交客和潜客\n"
        "步骤 3：分箱打分 — 用 qcut/cut 将连续值转为离散分数\n"
        "步骤 4：规则分层 — 用 if/elif/else 组合多个维度打标签"
    )

    pdf.section_title("7.4 成交用户分层逻辑（MECE 原则）")
    pdf.body_text(
        "MECE = Mutually Exclusive, Collectively Exhaustive\n"
        "= 相互独立、完全穷尽\n"
        "确保每个用户只属于一个类别，且所有用户都被覆盖。"
    )
    cols = ["用户标签", "命中条件", "业务含义"]
    w = [45, 60, 85]
    pdf.table_header(cols, w)
    rows = [
        ["核心高价值客户", "M>=5 且 R>=4", "高额消费+近期活跃，平台最优质资产"],
        ["重要挽留客户", "M>=5 且 R<=2", "大客户流失风险，需紧急召回"],
        ["高潜单次大客", "F=1, M>=4, I>=4", "首单高价+高互动，复购种子用户"],
        ["高互动普通客", "M<4 且 I>=4", "爱逛但消费低，适合内容种草"],
        ["低价值流失客", "F=1, M<=2, I<=2", "一次性低消费，营销价值低"],
        ["一般客户", "其他", "常规用户，维持自动化运营"],
    ]
    for i, row in enumerate(rows):
        pdf.table_row(row, w, highlight=(i % 2 == 0))

    pdf.section_title("7.5 潜客分层逻辑")
    pdf.body_text(
        "针对未成交用户（Frequency=0），传统交易维度失效，"
        "改用 Intent（互动次数）和 Session（停留时长）两个行为维度。\n"
        "新增新用户优先判断：首次出现 ≤7 天（待校准）的用户暂不判定为低价值，给予观察期。"
    )
    cols = ["用户标签", "命中条件", "业务含义"]
    w = [45, 60, 85]
    pdf.table_header(cols, w)
    rows = [
        ["新访客待观察", "首次出现 ≤7 天（待校准）", "初来乍到，尚未有足够时间转化"],
        ["核心高意向潜客", "I>=4 且 S>=4", "深度浏览+高频互动，临门一脚"],
        ["高时长静默潜客", "I<4 且 S>=4", "长时间停留但不互动，犹豫不决"],
        ["浅层高频交互客", "I>=4 且 S<=2", "快速点击秒退，人货不匹配"],
        ["尾部低价值流量", "I<=2 且 S<=2 且非新用户", "无效跳出（仅老用户），建议停止投放"],
        ["常规培育客群", "其他", "普通访客，长期培育"],
    ]
    for i, row in enumerate(rows):
        pdf.table_row(row, w, highlight=(i % 2 == 0))

    pdf.section_title("7.6 帕累托法则（二八定律）")
    pdf.body_text(
        "本项目的数据完美验证了帕累托法则：\n"
        "• 仅占大盘 8% 的头部用户（核心高价值 + 重要挽留）贡献了 30% 的总营收\n"
        "• 1.2% 的核心高意向潜客是转化漏斗最底部的"准买家"\n"
        "• 86.5% 的尾部流量为无效跳出\n\n"
        "启示：将有限的营销预算聚焦于高价值人群，而非"撒胡椒面"式投放。"
    )

    # ========== Chapter 8: Visualization ==========
    pdf.add_page()
    pdf.chapter_title("八", "数据可视化基础")

    pdf.section_title("8.1 matplotlib — Python 绑图基础库")
    pdf.body_text(
        "matplotlib 是 Python 最基础的绑图库，几乎所有 Python 可视化都基于它。"
        "通常配合 Pandas 使用。"
    )
    pdf.code_block(
        "import matplotlib.pyplot as plt\n"
        "\n"
        "# 基本绑图流程\n"
        "plt.figure(figsize=(10, 6))  # 创建画布\n"
        "plt.bar(x, y)                # 绘制柱状图\n"
        "plt.title('标题')             # 设置标题\n"
        "plt.xlabel('X轴标签')         # X轴标签\n"
        "plt.ylabel('Y轴标签')         # Y轴标签\n"
        "plt.show()                   # 显示图表"
    )

    pdf.section_title("8.2 seaborn — 统计可视化库")
    pdf.body_text(
        "seaborn 基于 matplotlib，提供更美观的默认样式和更简洁的 API，"
        "特别适合统计图表。"
    )
    pdf.code_block(
        "import seaborn as sns\n"
        "\n"
        "# 常用图表\n"
        "sns.histplot(data=df, x='monetary')     # 直方图\n"
        "sns.boxplot(data=df, x='group', y='AOV') # 箱线图\n"
        "sns.heatmap(corr_matrix)                 # 热力图"
    )

    pdf.section_title("8.3 Power BI — 商业智能工具")
    pdf.body_text(
        "Power BI 是微软的商业智能（BI）工具，无需编程即可创建交互式仪表板。\n\n"
        "核心功能：\n"
        "• 连接多种数据源（CSV、数据库、API 等）\n"
        "• 拖拽式创建图表和仪表板\n"
        "• 支持 DAX 公式进行复杂计算\n"
        "• 可导出用户 ID 列表用于运营执行\n\n"
        "注意：Power BI 的明细表导出上限通常为 3 万行（开启大模型可达 15 万行），"
        "大量数据导出需回到 SQL 工具。"
    )

    # ========== Chapter 9: A/B Test ==========
    pdf.add_page()
    pdf.chapter_title("九", "A/B 实验与统计检验")

    pdf.section_title("9.1 什么是 A/B 实验？")
    pdf.body_text(
        "A/B 实验是将用户随机分成两组（或多组），分别施加不同策略，"
        "然后用统计方法判断结果差异是否由策略导致（而非随机波动）。\n\n"
        "本项目模拟场景：\n"
        "• A 组（对照组）：5 元无门槛券\n"
        "• B 组（实验组）：满 100 减 20 元券\n"
        "• 比较指标：转化率 (CVR)、客单价 (AOV)、投资回报率 (ROI)"
    )

    pdf.section_title("9.2 核心业务指标")
    cols = ["指标", "公式", "含义"]
    w = [35, 75, 80]
    pdf.table_header(cols, w)
    rows = [
        ["CVR", "转化人数 / 总人数", "转化率，衡量策略对购买决策的影响"],
        ["AOV", "总营收 / 转化人数", "客单价，衡量每单平均金额"],
        ["ARPU", "总营收 / 总人数", "人均收入，衡量整体贡献"],
        ["ROI", "(营收-成本) / 成本", "投资回报率，衡量投入产出比"],
    ]
    for i, row in enumerate(rows):
        pdf.table_row(row, w, highlight=(i % 2 == 0))

    pdf.section_title("9.3 卡方检验 (Chi-Square Test)")
    pdf.body_text(
        "用途：检验两个分类变量之间是否存在显著关联。\n"
        "在本项目中：检验"A/B 分组"与"是否转化"之间是否有关联。"
    )
    pdf.code_block(
        "from scipy import stats\n"
        "\n"
        "# 构建列联表（交叉表）\n"
        "contingency_table = pd.crosstab(\n"
        "    df['group'],          # 行：A/B 组\n"
        "    df['is_converted']    # 列：是否转化\n"
        ")\n"
        "\n"
        "# 执行卡方检验\n"
        "chi2, p_val, dof, expected = stats.chi2_contingency(contingency_table)\n"
        "\n"
        "# 判断\n"
        "if p_val < 0.05:\n"
        "    print('差异显著')  # 拒绝原假设\n"
        "else:\n"
        "    print('差异不显著')  # 不能拒绝原假设"
    )
    pdf.tip_box("卡方检验的直觉理解",
        "原假设 H0：两组转化率没有差异（差异由随机波动导致）\n"
        "p 值：在 H0 为真的前提下，观察到当前差异（或更大差异）的概率\n"
        "• p < 0.05：概率很低，认为差异不是偶然的，拒绝 H0\n"
        "• p >= 0.05：概率不够低，无法排除随机波动，保留 H0")

    pdf.section_title("9.4 T 检验 (Welch's t-test)")
    pdf.body_text(
        "用途：检验两组数值型数据的均值是否存在显著差异。\n"
        "在本项目中：检验 A/B 两组已转化用户的客单价均值是否有差异。"
    )
    pdf.code_block(
        "# 提取两组已转化用户的订单金额\n"
        "aov_A = df.loc[converted_A, 'order_amount'].values\n"
        "aov_B = df.loc[converted_B, 'order_amount'].values\n"
        "\n"
        "# Welch's T 检验（不假设两组方差相等）\n"
        "t_stat, p_val = stats.ttest_ind(\n"
        "    aov_A, aov_B,\n"
        "    equal_var=False  # 关键参数：不等方差\n"
        ")\n"
        "\n"
        "if p_val < 0.05:\n"
        "    print('客单价差异显著')"
    )
    pdf.body_text(
        "为什么用 Welch's t-test 而不是 Student's t-test？\n"
        "• Student's t-test 假设两组方差相等\n"
        "• Welch's t-test 不需要这个假设，更稳健\n"
        "• 当两组样本量或方差差异较大时，Welch's 更准确"
    )

    pdf.section_title("9.5 p 值的正确解读")
    pdf.warning_box(
        "p 值不是"两组相同的概率"！\n\n"
        "正确理解：假设两组没有差异（H0 为真），观察到当前结果（或更极端结果）的概率。\n\n"
        "• p < 0.01：非常显著（**）\n"
        "• p < 0.05：显著（*）\n"
        "• p >= 0.05：不显著\n\n"
        "0.05 是常用的阈值，但不是绝对标准。实际业务中需结合效应大小和业务影响综合判断。"
    )

    # ========== Chapter 10: Business Thinking ==========
    pdf.add_page()
    pdf.chapter_title("十", "业务思维与落地应用")

    pdf.section_title("10.1 帕累托法则（二八定律）")
    pdf.body_text(
        "核心思想：80% 的结果来自 20% 的原因。\n"
        "在电商中的表现：\n"
        "• 20% 的核心用户贡献 80% 的营收\n"
        "• 本项目数据：8% 的头部用户贡献 30% 营收\n\n"
        "应用：将营销预算倾斜给高价值人群，而非平均分配。"
    )

    pdf.section_title("10.2 MECE 原则")
    pdf.body_text(
        "MECE = Mutually Exclusive, Collectively Exhaustive\n"
        "= 相互独立、完全穷尽\n\n"
        "• 相互独立：每个用户只能属于一个类别（不重复）\n"
        "• 完全穷尽：所有用户都必须被归入某个类别（不遗漏）\n\n"
        "在用户分层中的应用：确保 if/elif/else 的条件互斥且覆盖所有情况。"
    )

    pdf.section_title("10.3 ROI（投资回报率）")
    pdf.body_text(
        "ROI = (收益 - 成本) / 成本\n\n"
        "在本项目中的应用：\n"
        "• ROI = (总营收 - 总发券成本) / 总发券成本\n"
        "• A 组：5 元无门槛券，单券成本低，但客单价也低\n"
        "• B 组：满 100 减 20，单券成本高，但客单价翻倍\n"
        "• 需要综合判断哪种策略的 ROI 更高"
    )

    pdf.section_title("10.4 CVR 与 AOV 的权衡")
    pdf.body_text(
        "转化率 (CVR) 和客单价 (AOV) 往往存在此消彼长的关系：\n"
        "• 降低门槛 → CVR 上升，但 AOV 可能下降\n"
        "• 提高门槛 → AOV 上升，但 CVR 可能下降\n\n"
        "最优策略是找到 CVR × AOV 的最大值（即总营收最大）。"
    )

    pdf.section_title("10.5 运营策略落地")
    pdf.body_text(
        "基于用户分层的差异化运营（具体执行前需结合 CAC 与 LTV 验证投入产出比）："
    )
    cols = ["用户类型", "策略", "前提条件"]
    w = [40, 45, 105]
    pdf.table_header(cols, w)
    rows = [
        ["核心高价值客户", "VIP 权益锁定", "VIP 权益成本 < 该客群 LTV"],
        ["重要挽留客户", "紧急召回", "单客回访成本 < 预期召回收益"],
        ["高潜单次大客", "首单转复购", "需确认推荐系统改造成本与券预算"],
        ["高互动普通客", "内容种草", "需确认引流款补贴预算"],
        ["低价值流失客", "战略放弃", "无额外成本，直接节省营销支出"],
        ["核心高意向潜客", "极速转化", "需确认券面额与预期转化率的 ROI"],
        ["尾部低价值流量", "停止投放", "需先区分新用户与老访客"],
    ]
    for i, row in enumerate(rows):
        pdf.table_row(row, w, highlight=(i % 2 == 0))

    pdf.section_title("10.6 A/B 实验方法论演示（非真实实验结论）")
    pdf.body_text(
        "说明：本节为 A/B 实验方法论的完整演示，CVR（11%/13.5%）与 AOV（60/125 元）"
        "参数为模拟占位值，不代表真实实验结论。落地前需基于真实业务数据，通过功效分析"
        "确定最小样本量并设计实验。\n\n"
        "在模拟设定下，满减券（B 组）在 CVR 和 AOV 两个维度均优于无门槛券（A 组）：\n\n"
        "• 转化率提升：满减制造了消费目标感，CVR 从 11% → 13.5%\n"
        "• 客单价跃迁：凑单效应使 AOV 从 60 元 → 125 元（涨幅 100%+）\n"
        "• 成本可控：虽然单券成本 5→20 元，但 AOV 提升使利润空间更充裕\n\n"
        "落地建议（需结合真实成本数据验证）：\n"
        "• 核心高价值 / 一般客户 → 满减券优先（需确认客单价分布能支撑满减门槛）\n"
        "• 重要挽留 / 高潜单次大客 → 保留无门槛券（需测算单客 LTV 是否覆盖券成本）\n"
        "• 高互动普通客 → 混合策略持续 A/B 测试（需功效分析确定最小样本量）"
    )

    # ========== Chapter 11: Limitations ==========
    pdf.add_page()
    pdf.chapter_title("十一", "模型局限性与使用前提")

    pdf.body_text(
        "本项目的分析框架和分层逻辑具备可复用性，但以下局限性在使用前需明确："
    )

    pdf.section_title("参数校准")
    pdf.body_text(
        "当前分层阈值（如 Intent 中 cart 与 view 的权重比、M 的 6 分档边界值、"
        "潜客分层的 I/S 阈值）均基于本数据集的分布特征设定。迁移到其他业务场景时，"
        "需基于目标数据的分布重新校准，不可直接套用。"
    )

    pdf.section_title("A/B 实验")
    pdf.body_text(
        "A/B 实验章节为方法论框架演示，其中 CVR（11% / 13.5%）与 AOV（60 元 / 125 元）"
        "参数为模拟占位值，不代表真实实验结论。落地前需通过功效分析（Power Analysis）"
        "确定最小样本量，并基于真实业务数据设计实验。"
    )

    pdf.section_title("季节性偏差")
    pdf.body_text(
        "数据覆盖 2019 年 10-11 月（含双十一促销期），F=1 用户中可能包含大量大促一次性"
        "购买用户，高客单价可能受囤货行为影响。将模型应用于日常运营时，需评估大促期间"
        "数据对分层结果的干扰程度。"
    )

    pdf.section_title("营销成本")
    pdf.body_text(
        "各用户标签对应的营销建议（如人工客服回访、VIP 权益建设）未附带成本测算。"
        "实际执行前需结合业务方的单客获客成本（CAC）与用户生命周期价值（LTV）"
        "计算投入产出比。"
    )

    # ========== Appendix ==========
    pdf.add_page()
    pdf.chapter_title("附", "附录：关键函数速查表")

    pdf.section_title("NumPy 函数")
    cols = ["函数", "用途", "示例"]
    w = [55, 60, 75]
    pdf.table_header(cols, w)
    rows = [
        ["np.random.seed(n)", "固定随机种子", "seed(42) 保证可复现"],
        ["np.random.choice()", "随机选择", "从 A/B 中随机抽取"],
        ["np.random.binomial()", "二项分布", "模拟转化(0/1)"],
        ["np.random.normal()", "正态分布", "模拟订单金额"],
        ["np.clip()", "截断数组", "限制最低金额为5元"],
        ["np.inf", "正无穷大", "分箱时作为上界"],
    ]
    for i, row in enumerate(rows):
        pdf.table_row(row, w, highlight=(i % 2 == 0))

    pdf.ln(4)
    pdf.section_title("Pandas 函数")
    cols = ["函数", "用途", "示例"]
    w = [55, 60, 75]
    pdf.table_header(cols, w)
    rows = [
        ["pd.DataFrame()", "创建数据框", "从字典创建表格"],
        ["df.groupby().agg()", "分组聚合", "按组统计均值/求和"],
        ["pd.qcut()", "等频分箱", "按分位数分成5组"],
        ["pd.cut()", "等距分箱", "按自定义区间分组"],
        ["df.apply(func, axis=1)", "逐行应用函数", "自定义分层标签"],
        ["df.fillna()", "填充空值", "空值填0或大数"],
        ["pd.crosstab()", "交叉表", "卡方检验的输入"],
        [".dt.days", "提取天数", "时间差转天数"],
        [".dt.total_seconds()", "提取总秒数", "时长转秒数"],
        [".rank()", "排名", "处理重复值后分箱"],
    ]
    for i, row in enumerate(rows):
        pdf.table_row(row, w, highlight=(i % 2 == 0))

    pdf.ln(4)
    pdf.section_title("SQL 关键语法")
    cols = ["语法", "用途", "示例"]
    w = [55, 60, 75]
    pdf.table_header(cols, w)
    rows = [
        ["CASE WHEN...THEN", "条件判断", "筛选 purchase 行为"],
        ["GROUP BY", "分组", "按 user_id 聚合"],
        ["WITH (CTE)", "临时表", "拆分复杂查询"],
        ["LEFT JOIN", "左连接", "合并两个特征表"],
        ["USING SAMPLE", "抽样", "1% 伯努利抽样"],
        ["COPY TO", "导出", "导出 CSV 文件"],
        ["UNION ALL", "合并结果", "上下拼接两张表"],
        ["MAX/COUNT/SUM", "聚合函数", "统计最大值/计数/求和"],
    ]
    for i, row in enumerate(rows):
        pdf.table_row(row, w, highlight=(i % 2 == 0))

    pdf.ln(4)
    pdf.section_title("scipy 统计检验")
    cols = ["函数", "用途", "适用场景"]
    w = [55, 60, 75]
    pdf.table_header(cols, w)
    rows = [
        ["chi2_contingency()", "卡方检验", "分类变量关联性(如CVR差异)"],
        ["ttest_ind(equal_var=False)", "Welch's T检验", "两组均值差异(如AOV差异)"],
    ]
    for i, row in enumerate(rows):
        pdf.table_row(row, w, highlight=(i % 2 == 0))

    # Save
    output_path = r"D:\实战项目\Dataanalysis\电商数据分析实战教程.pdf"
    pdf.output(output_path)
    print(f"PDF generated: {output_path}")


if __name__ == "__main__":
    build_pdf()
