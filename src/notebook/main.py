import Agently
from datetime import datetime
from dotenv import load_dotenv
import os
import sqlite3  # 添加 SQLite 连接库
import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

load_dotenv()

agent_factory = Agently.AgentFactory()

# 创建 Agent 实例
agent = (
    Agently.create_agent()
    .set_settings("current_model", "OAIClient")
    .set_settings("model.OAIClient.auth", { "api_key": f"{os.getenv('DEEPSEEK_API')}" })
    .set_settings("model.OAIClient.url", f"{os.getenv('DEEPSEEK_URL')}")
    .set_settings("model.OAIClient.options", { "model": "deepseek-chat" })
)

meta_data = [
    {
        "table_name": "sales_purchase",
        "columns": [
            { "name": "company_name", "desc": "公司名称", "data type": "string", "example": "国贸能化" },
            { "name": "customer_name", "desc": "客户名称", "data type": "string", "example": "客户A" },
            { "name": "sales_amount", "desc": "销售金额", "data type": "float", "example": 32456.0 },
            { "name": "purchase_amount", "desc": "采购金额", "data type": "float", "example": 30000.0 },
            { "name": "gross_profit", "desc": "毛利润", "data type": "float", "example": 24567.0 },
            { "name": "gross_margin", "desc": "毛利率", "data type": "float", "example": 0.0756 },
            { "name": "sales_settlement_date", "desc": "销售结算日期", "data type": "date", "example": "2024-03-15" },
            { "name": "purchase_settlement_date", "desc": "采购结算日期", "data type": "date", "example": "2024-01-05" },
            { "name": "product_category", "desc": "商品类别", "data type": "string", "example": "煤炭" },
        ],
    },
    {
        "table_name": "coal_inventory",
        "columns": [
            { "name": "warehouse_name", "desc": "仓库名称", "data type": "string", "example": "武汉港仓库" },
            { "name": "product_type", "desc": "商品类型", "data type": "string", "example": "煤炭" },
            { "name": "inventory_weight", "desc": "库存重量", "data type": "float", "example": 10000 },
            { "name": "estimated_inventory_value", "desc": "暂估库存货值", "data type": "float", "example": 5000000 },
            { "name": "advance_payment", "desc": "预付货款", "data type": "float", "example": 4000000 },
            { "name": "update_date", "desc": "更新日期", "data type": "date", "example": "2024-03-15" },
        ],
    },
    {
        "table_name": "steel_inventory",
        "columns": [
            { "name": "warehouse_name", "desc": "仓库名称", "data type": "string", "example": "武汉钢材仓库" },
            { "name": "product_type", "desc": "商品类型", "data type": "string", "example": "钢材" },
            { "name": "inventory_quantity", "desc": "库存量", "data type": "float", "example": 5000 },
            { "name": "inventory_value", "desc": "存货金额余额", "data type": "float", "example": 10000000 },
            { "name": "update_date", "desc": "更新日期", "data type": "date", "example": "2024-03-15" },
        ],
    },
    {
        "table_name": "agency_business",
        "columns": [
            { "name": "company_name", "desc": "公司名称", "data type": "string", "example": "国贸能化" },
            { "name": "customer_name", "desc": "客户名称", "data type": "string", "example": "客户A" },
            { "name": "product_category", "desc": "商品类别", "data type": "string", "example": "煤炭" },
            { "name": "customer_inventory_value", "desc": "客户存货金额余额", "data type": "float", "example": 5000000 },
            { "name": "update_date", "desc": "更新日期", "data type": "date", "example": "2024-03-15" },
        ],
    },
    {
        "table_name": "receivables_business",
        "columns": [
            { "name": "company_name", "desc": "公司名称", "data type": "string", "example": "国贸能化" },
            { "name": "customer_name", "desc": "客户名称", "data type": "string", "example": "客户A" },
            { "name": "receivables_balance", "desc": "应收余额", "data type": "float", "example": 3000000 },
            { "name": "update_date", "desc": "更新日期", "data type": "date", "example": "2024-03-15" },
        ],
    },
    {
        "table_name": "ageing_business",
        "columns": [
            { "name": "company_name", "desc": "公司名称", "data type": "string", "example": "国贸能化" },
            { "name": "customer_name", "desc": "客户名称", "data type": "string", "example": "客户A" },
            { "name": "account_amount", "desc": "账款金额", "data type": "float", "example": 1000000 },
            { "name": "ageing", "desc": "账龄", "data type": "string", "example": "1-30天" },
            { "name": "is_anomaly", "desc": "是否异常", "data type": "boolean", "example": "否" },
        ],
    },
]

# 连接到SQLite数据库
connection = sqlite3.connect(os.path.join(os.path.dirname(__file__), 'mydatabase.db'))  # 使用相对路径连接数据库

def prepare_sql_query(sql_query):
    """
    清理并准备SQL查询字符串。
    
    参数:
    sql_query (str): 原始的多行SQL查询字符串。
    
    返回:
    str: 清理后的单行SQL查询字符串。
    """
    # 去除字符串两端的空白字符
    sql_query = sql_query.strip()
    # 替换字符串中的换行符和多余的空格
    sql_query = ' '.join(sql_query.split())
    return sql_query.replace("```", "")
# 创建 Streamlit 页面
st.title("数据查询与可视化")

# 用户输入
user_input = st.text_input("请输入您的问题:")

if st.button("查询"):
    if user_input:
        st.write("[Thinking]:")
        result = (
            agent
            .input(user_input)
            .info({ "database meta data": meta_data })
            .info({ "current date": datetime.now().date() })
            .instruct([
                "Generate SQL for SQLite database according {database meta data} to answer {input}",
                "Language: 中文",
            ])
            .segment(
                "thinkings",  # 添加思过程的输出
                ("String", "Your thinkings step by step about how to query data to answer {input}"),
                lambda data: st.write(data),  # 输出思考过程
                is_streaming=True
            )
            .segment(
                "SQL",
                ("String", "SQL String without explanation"),
                lambda data: data.strip().replace("```", "").replace("\n", ""),  # 去掉反引号和换行符
                is_streaming=False
            )
            .start()
        )

        # 打印结果以调试
        st.write("生成的 SQL 查询:", result)

        # 提取 thinking 和 SQL
        thinking = result.get("thinkings", "")
        sql_query = prepare_sql_query(result.get("SQL", ""))  # 去掉多余的空格

        # 输出思考过程和 SQL 查询
        st.write("思考过程:", thinking)
        st.write("生成的 SQL 查询:", sql_query)

        # 确保 sql_query 是字符串
        if isinstance(sql_query, str) and sql_query:
            # 使用 pandas 查询数据库
            df = pd.read_sql_query(sql_query, connection)  # 使用数据库连接

            # 显示查询到的 DataFrame
            st.write(df)  # 添加此行以显示 DataFrame

            # 检查 DataFrame 是否为空
            if df.empty:
                st.warning("查询结果为空，请检查 SQL 查询。")
            else:
                # 设置中文字体
                plt.rcParams['font.family'] = 'SimHei'  # 'SimHei'是黑体的字体名
                plt.rcParams['font.size'] = 12
                plt.rcParams['axes.unicode_minus'] = False  

                # 动态获取 x 和 y 的列名
                x_columns = df.select_dtypes(include=['object']).columns  # 获取所有字符串列
                y_columns = df.select_dtypes(include=['number']).columns  # 获取所有数值列

                if not y_columns.empty:
                    # 如果有字符串列，使用第一个字符串列；否则使用索引
                    x_column = x_columns[0] if not x_columns.empty else df.index  
                    
                    # 选择最后一个���值列
                    last_y_column = y_columns[-1]  

                    # 绘制条形图
                    plt.figure(figsize=(10, 6))
                    sns.barplot(data=df, x=x_column, y=last_y_column)  # 使用最后一个数值列
                    plt.title(f"{last_y_column} 的条形图")  # 添加标题
                    st.pyplot(plt)
                else:
                    st.error("没有可用的列进行绘图，请检查查询结果。")
        else:
            st.error("生成的 SQL 查询无效，请检查输入。")

# 关闭数据库连接
connection.close()  # 添加关闭连接