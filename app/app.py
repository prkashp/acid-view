import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import datetime
from snowflake_connector import fetch_data
from processor import *
# from app.sidebar_obsolete import sidebar
DATA_TYPES = {'DATA_SOURCE': np.str_, 'PRIORITY': np.str_, 'CHECK_NAME': np.str_,'TABLE_NAME': np.str_,'STATUS':np.int8}
REMOTE_CSS_URL = "https://cdnjs.cloudflare.com/ajax/libs/semantic-ui/2.4.1/semantic.min.css"
# SQL query path
st.set_page_config(page_title="Acid View", layout="wide", page_icon="./app/data/happy.png", initial_sidebar_state="expanded")

def css():
    st.markdown(f'<link href="{REMOTE_CSS_URL}" rel="stylesheet">', unsafe_allow_html=True)
    try:
        with open("./app/style.css") as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    except:
        raise Exception("File not found", os.getcwd(), os.listdir())

def table_cards(df, view_details):

    table_scorecard = """
    <div class="ui five small statistics">
        <div class="grey statistic">
            <div class="value">"""+str(df[df['TABLE_TYPE'] == 'BASE TABLE']['TABLE_ID'].count())+"""
            </div>
            <div style='color:grey;' class="label" >
                Tables
            </div>
        </div>
        <div class="grey statistic">
            <div class="value">"""+str(df[df['TABLE_TYPE'] == 'VIEW']['TABLE_ID'].count())+"""
            </div>
            <div class="label">
                Views
            </div>
        </div>
        <div class="grey statistic">
            <div class="value">"""+str(df[df['TABLE_TYPE'] == 'MATERIALIZED VIEW']['TABLE_ID'].count())+"""
            </div>
            <div class="label">
                Materialized Views
            </div>
        </div>
        <div class="grey statistic">
            <div class="value">
                """+human_format(df['ROW_COUNT'].sum())+"""
            </div>
            <div class="label">
                Rows
            </div>
        </div>
        <div class="grey statistic">
            <div class="value">
                """+human_bytes(df['BYTES'].sum())+" "+human_bytes_text(df['BYTES'].sum())+"""
            </div>
            <div class="label">
                Size
            </div>
        </div>
    </div>    
"""

    table_scorecard += """<br><br><br><div id="mydiv" class="ui centered cards">"""


    for index, row in df.iterrows():
        table_scorecard += """
<div class="card"">   
    <div class=" content """+header_bg(row['TABLE_TYPE'])+"""">
            <div class=" header smallheader">"""+row['TABLE_NAME']+"""</div>
<div class="meta smallheader">"""+row['TABLE_CATALOG']+"."+row['TABLE_SCHEMA']+"""</div>
</div>
<div class="content">
    <div class="description"><br>
        <div class="column kpi number">"""+human_format(row['ROW_COUNT'])+"""<br>
            <p class="kpi text">Rows</p>
        </div>
        <div class="column kpi number">"""+human_bytes(row['BYTES'])+"""<br>
            <p class="kpi text">"""+human_bytes_text(row['BYTES'])+"""</p>
        </div>
        <div class="column kpi number">"""+"{0:}".format(row['COLUMN_COUNT'])+"""<br>
            <p class="kpi text">Columns</b>
        </div>
    </div>
</div>
<div class="extra content">
    <div class="meta"><i class="table icon"></i> Table Type: """+(row['TABLE_TYPE'])+"""</div>
    <div class="meta"><i class="user icon"></i> Owner: """+str(row['TABLE_OWNER'])+""" </div>
    <div class="meta"><i class="calendar alternate outline icon"></i> Created On:
        """+str(pd.to_datetime(row['CREATED'], dayfirst=True, format='mixed').date())+"""</div>
</div>
<div class="extra content" """+view_details+""">
    <div class="meta"><i class="history icon"></i> Time Travel: """+str((row['RETENTION_TIME'])).strip(".0")+"""</div>
    <div class="meta"><i class="edit icon"></i> Last Altered: """+str(pd.to_datetime(row['LAST_ALTERED'], dayfirst=True, format='mixed').date())+"""</div>
    <div class="meta"><i class="calendar times outline icon"></i> Transient: """+str(row['IS_TRANSIENT'])+""" </div>
    <div class="meta"><i class="th icon"></i> Auto Clustering: """+str(row['AUTO_CLUSTERING_ON'])+""" </div>
    <div class="meta"><i class="key icon"></i> Clustering Key: """+str(row['IS_TRANSIENT'])+""" </div>
    <div class="meta"><i class="comment alternate outline icon"></i> Comment: """+str(row['IS_TRANSIENT'])+""" </div>
</div>
</div>
        """

    st.markdown(table_scorecard, unsafe_allow_html=True)

# @st.cache_data
def sidebar_v2(df):
    df_reset = df

    st.markdown("""
<style>
    [data-testid=stSidebar] {
        background-color: #14665e91;
    }
    [data-testid=stSidebarUserContent] {
        padding-top: 2rem;
    }
</style>
""", unsafe_allow_html=True)
    
    #session state 
    if "selectbox_table_name" not in st.session_state:
        st.session_state["selectbox_date_range"]=0
        st.session_state["selectbox_table_name"] = 1
        st.session_state["selectbox_data_source"] = 2
        st.session_state["selectbox_priority"] = 3
        st.session_state["selectbox_check_name"] = 4
        
    def reset_button():
        st.session_state["selectbox_date_range"] += st.session_state["selectbox_date_range"] 
        st.session_state["selectbox_table_name"] += st.session_state["selectbox_table_name"] 
        st.session_state["selectbox_data_source"] += st.session_state["selectbox_data_source"]
        st.session_state["selectbox_priority"] += st.session_state["selectbox_priority"]
        st.session_state["selectbox_check_name"] += st.session_state["selectbox_check_name"]

    # Toggle for more details on cards
    # render_more_details = st.sidebar.toggle("Enable Details", help='When this toggle is on it shows more details of table objects')
    # expanded_view = "" if render_more_details else """style="display: none;" """

    # Order by the table cards
    date_range = st.sidebar.selectbox(":clock3: Filter",("Last 7 days",'Last 30 days','Last 365 days'), key=st.session_state["selectbox_date_range"])
    # order by cases
    col_time_filter = {
        "Last 7 days" : 7,
        "Last 30 days" : 30,
        "Last 365 days" : 365
    }
    df = df[(df['DATE']>np.datetime64('today') - np.timedelta64(col_time_filter[date_range],'D')) & (df['DATE']<np.datetime64('today'))]  
    
    # print(df[df['DATE']<np.datetime64('today')])
    # Get processed data
    col_db, col_schema, col_owner, col_table_type = preprocess_data(df)

    # Filter table cards
    selectbox_db = st.sidebar.selectbox("Table Name", col_db, index=len(col_db)-1, key=st.session_state["selectbox_table_name"])
    df = df.loc[df["TABLE_NAME"].isin(col_db)] if selectbox_db == "All" else df.loc[df["TABLE_NAME"]==selectbox_db]

    selectbox_schema = st.sidebar.selectbox("Source DB", col_schema, index=len(col_schema)-1, key=st.session_state["selectbox_data_source"])
    df = df.loc[df["DATA_SOURCE"].isin(col_schema)] if selectbox_schema == "All" else df.loc[df["DATA_SOURCE"]==selectbox_schema]

    selectbox_owner = st.sidebar.selectbox("Priority", col_owner, index=len(col_owner)-1, key=st.session_state["selectbox_priority"])
    df = df.loc[df["PRIORITY"].isin(col_owner)] if selectbox_owner == "All" else df.loc[df["PRIORITY"]==selectbox_owner]

    st.markdown("""
<style>
    span[data-baseweb="tag"] {
        background-color: #c7c53cd3;
    }
</style>
""", unsafe_allow_html=True)
    
    selectbox_type = st.sidebar.multiselect("Check Type", col_table_type, col_table_type, key=st.session_state["selectbox_check_name"])
    df = df.loc[df["CHECK_NAME"].isin(col_table_type)] if len(selectbox_type) <= 0 else df.loc[df["CHECK_NAME"].isin(selectbox_type)]

    # Reset filter button
    reset = st.sidebar.button(label="Clear Selection", on_click=reset_button)
    df = df_reset if reset else df

    return df

def cards(df):
    col1, col2, col3 = st.columns([2,2,1], gap='large')
    col1.metric('Pass ✅', value=(df['STATUS']==0).sum()) # ☀️
    col2.metric("Warn ⚠️", value=(df['STATUS']==2).sum()) # ☁️
    col3.metric("Fail ❌", value=(df['STATUS']==1).sum()) # ⛈️

def status(df):
    if df['STATUS']== 0:
        return 'PASS'
    elif df['STATUS']==1:
        return 'FAIL'
    elif df['STATUS']==2:
        return 'WARN'

def search():
    # https://blog.streamlit.io/create-a-search-engine-with-streamlit-and-google-sheets/
    pass

# using dayfirst is not efficient, build process to fix data at csv source
def main():
    
    # st.title(":rainbow[MetaFlake View]")
    st.markdown("<h1>Acid View</h1>", unsafe_allow_html=True)

    css()
    # Fetch data from Snowflake
    data = None
    try: 
        # data = fetch_data()
        pass
    except:
        print('snowflake inaccessible')

    df = pd.read_csv('./app/data/records.csv', dtype=DATA_TYPES, parse_dates=['DATE']) if not data else data
    
    # df.set_index('DATE', inplace=True)
    # df.index = pd.to_datetime(df.DATE)
    df_final = sidebar_v2(df)
    cards(df_final)
    df_final['ST'] = df_final.apply(status,axis=1)
    df_summary=df_final.groupby(['DATE','ST'])['TABLE_NAME'].count().reset_index()
    df_summary.columns=['DATE','STATUS','COUNT']
    df_chart=df_summary.pivot(index='DATE', columns='STATUS', values='COUNT')
    
    # st.bar_chart(df_chart)
    st.bar_chart(data=df_chart,color=['#ff4500', '#008080', '#e8e82a'])
    

    df = px.data.gapminder().query("continent == 'Oceania'")
    print(df)
    fig = px.bar(df, x='year', y='pop',
                hover_data=['lifeExp', 'gdpPercap'], color='country',
                labels={'pop':'population of Canada'}, height=400)
    st.plotly_chart(fig)
if __name__ == "__main__":
    main()
